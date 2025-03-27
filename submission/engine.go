package submission

import "C"
import (
	"assign2/utils"
	"assign2/wg"
	"container/heap"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

type Engine struct {
	wg              *wg.WaitGroup
	coordinatorChan chan *InstrumentRequest // allows clients to request for coordinator service
	// cancel          context.CancelFunc
}

func (e *Engine) Init(ctx context.Context, wg *wg.WaitGroup) {
	e.wg = wg
	// context, cancel := context.WithCancel(ctx)
	// e.cancel = cancel

	e.coordinatorChan = make(chan *InstrumentRequest)

	wg.Add(1)
	go coordinator(ctx, wg, e.coordinatorChan)
}

// Engine should shutdown once all goroutines shutdowns
func (e *Engine) Shutdown(ctx context.Context) {
	e.wg.Wait()

	// by this point, all clients have closed connections.
	// we shutdown the coordinator and instrument routines
	// e.cancel()
}

func (e *Engine) Accept(ctx context.Context, conn net.Conn) {
	e.wg.Add(2)

	go func() {
		defer e.wg.Done()
		<-ctx.Done()
		conn.Close()
	}()

	// This goroutine handles the connection.
	go func() {
		defer e.wg.Done()
		handleConn(conn, e.coordinatorChan)
	}()
}

func handleConn(conn net.Conn, coordinatorChan chan *InstrumentRequest) {
	defer conn.Close()

	// client sends each input to the central goroutine through its outgoing channel.
	// The central goroutine sends back the relevant instrument through the incoming channel.
	instrumentRequest := InstrumentRequest{
		outChan: make(chan utils.Input),
		inChan:  make(chan *Instrument),
	}

	idToInstrument := make(map[uint32]chan OrderRequest)

	for {
		in, err := utils.ReadInput(conn)
		if err != nil {
			if err != io.EOF {
				_, _ = fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			}
			return
		}

		orderRequest := OrderRequest{
			activeOrder: in,
			doneChan:    make(chan struct{}),
		}

		switch in.OrderType {
		case utils.InputCancel:
			fmt.Fprintf(os.Stderr, "Got cancel ID: %v\n", in.OrderId)
			cancelChan := idToInstrument[in.OrderId]
			cancelChan <- orderRequest

		default:
			fmt.Fprintf(os.Stderr, "Got order: %c %v x %v @ %v ID: %v\n",
				in.OrderType, in.Instrument, in.Count, in.Price, in.OrderId)

			// poll to request service from central goroutine
			coordinatorChan <- &instrumentRequest

			// send the input to the central goroutine to process
			instrumentRequest.outChan <- in

			// receive the instrument from the central goroutine
			instrument := <-instrumentRequest.inChan

			// send the input to the corresponding instrument channel
			// then map id to that channel
			if in.OrderType == utils.InputBuy {
				instrument.buyChan <- orderRequest
				idToInstrument[in.OrderId] = instrument.cancelBuyChan
			} else {
				instrument.sellChan <- orderRequest
				idToInstrument[in.OrderId] = instrument.cancelSellChan
			}
		}

		<-orderRequest.doneChan // prevent client from sending the next request before the current request is processed
	}
}

func GetCurrentTimestamp() int64 {
	return time.Now().UnixNano()
}

// A centralize goroutine that
// 1. manages the map of instrument
// 2. processes each client request and passes the relevant instrument channel back to client
func coordinator(ctx context.Context, wg *wg.WaitGroup, coordinatorChan chan *InstrumentRequest) {
	defer wg.Done()

	instrument_map := make(map[string]*Instrument)

	for {
		select {
		case <-ctx.Done():
			return

		case request := <-coordinatorChan: // a client requests service

			input := <-request.outChan

			if _, ok := instrument_map[input.Instrument]; !ok {
				// instrument not exist, create instrument and
				// goroutines to handle each queue in the instrument

				new_instrument := Instrument{
					buyChan:        make(chan OrderRequest),
					cancelBuyChan:  make(chan OrderRequest),
					sellChan:       make(chan OrderRequest),
					cancelSellChan: make(chan OrderRequest),
				}

				new_instrument.init(ctx, wg)

				instrument_map[input.Instrument] = &new_instrument
			}

			request.inChan <- instrument_map[input.Instrument]
		}
	}
}

// A request to the central coordinator to query for instrument
type InstrumentRequest struct {
	outChan chan utils.Input
	inChan  chan *Instrument
}

type Instrument struct {
	buyChan        chan OrderRequest // channel to process active buy orders
	cancelBuyChan  chan OrderRequest // channel to cancel buy orders
	sellChan       chan OrderRequest // channel to process active sell orders
	cancelSellChan chan OrderRequest // channel to cancel sell orders
}

// Request to process the order
type OrderRequest struct {
	activeOrder utils.Input
	doneChan    chan struct{}
}

func (ins *Instrument) init(ctx context.Context, wg *wg.WaitGroup) {

	addChan := make(chan OrderRequest)

	wg.Add(2)
	go ins.sellInstrumentManager(ctx, wg, addChan)
	go ins.buyInstrumentManager(ctx, wg, addChan)
}

func (ins *Instrument) sellInstrumentManager(ctx context.Context, wg *wg.WaitGroup, addChan chan OrderRequest) {
	defer wg.Done()

	restingSellOrders := &MinHeap{}
	idToOrder := make(map[uint32]*Order)

	for {
		select {
		case <-ctx.Done():
			return

		case request := <-ins.buyChan:
			activeBuyOrder := &request.activeOrder

			// match with resting sell orders
			for restingSellOrders.Len() > 0 && activeBuyOrder.Count > 0 {
				restingSellOrder := (*restingSellOrders)[0] // peek the top

				if !restingSellOrder.Valid {
					heap.Pop(restingSellOrders) // remove invalid order
					continue
				}

				isMatchingPrice := activeBuyOrder.Price >= restingSellOrder.Price
				if !isMatchingPrice {
					break // no other resting sell orders match
				}

				executeTransaction(activeBuyOrder, restingSellOrder)

				if restingSellOrder.Count > 0 {
					restingSellOrder.ExecutionID += 1
				} else {
					restingSellOrder.Valid = false
					heap.Pop(restingSellOrders)
				}
			}

			// send leftover to other goroutine
			if activeBuyOrder.Count > 0 {
				addChan <- request
			} else {
				close(request.doneChan)
			}

		case request := <-addChan:

			// add matched sell order to the resting queue
			timestamp := GetCurrentTimestamp()

			sellOrder := Order{
				OrderID:     request.activeOrder.OrderId,
				Price:       request.activeOrder.Price,
				Count:       request.activeOrder.Count,
				ExecutionID: 1,
				Timestamp:   uint32(timestamp),
				Valid:       true,
			}

			idToOrder[sellOrder.OrderID] = &sellOrder
			heap.Push(restingSellOrders, &sellOrder)

			utils.OutputOrderAdded(request.activeOrder, timestamp)
			close(request.doneChan)

		case request := <-ins.cancelSellChan:
			orderId := request.activeOrder.OrderId
			order := idToOrder[orderId]
			isValid := order.Valid
			if isValid {
				order.Valid = false
			}

			utils.OutputOrderDeleted(request.activeOrder, isValid, GetCurrentTimestamp())
			close(request.doneChan)

		}
	}
}

func (ins *Instrument) buyInstrumentManager(ctx context.Context, wg *wg.WaitGroup, addChan chan OrderRequest) {
	defer wg.Done()

	restingBuyOrders := &MaxHeap{}
	idToOrder := make(map[uint32]*Order)

	for {
		select {
		case <-ctx.Done():
			return

		case request := <-ins.sellChan:
			activeSellOrder := &request.activeOrder

			// match with resting sell orders
			for restingBuyOrders.Len() > 0 && activeSellOrder.Count > 0 {
				restingBuyOrder := (*restingBuyOrders)[0] // peek the top

				if !restingBuyOrder.Valid {
					heap.Pop(restingBuyOrders) // remove invalid order
					continue
				}

				isMatchingPrice := activeSellOrder.Price <= restingBuyOrder.Price
				if !isMatchingPrice {
					break // no other resting buy orders match
				}

				executeTransaction(activeSellOrder, restingBuyOrder)

				if restingBuyOrder.Count > 0 {
					restingBuyOrder.ExecutionID += 1
				} else {
					restingBuyOrder.Valid = false
					heap.Pop(restingBuyOrders)
				}
			}

			// send leftover to other goroutine
			if activeSellOrder.Count > 0 {
				addChan <- request
			} else {
				close(request.doneChan)
			}

		case request := <-addChan:

			// add matched sell order to the resting queue
			timestamp := GetCurrentTimestamp()

			buyOrder := Order{
				OrderID:     request.activeOrder.OrderId,
				Price:       request.activeOrder.Price,
				Count:       request.activeOrder.Count,
				ExecutionID: 1,
				Timestamp:   uint32(timestamp),
				Valid:       true,
			}

			idToOrder[buyOrder.OrderID] = &buyOrder
			heap.Push(restingBuyOrders, &buyOrder)

			utils.OutputOrderAdded(request.activeOrder, timestamp)
			close(request.doneChan)

		case request := <-ins.cancelBuyChan:
			orderId := request.activeOrder.OrderId
			order := idToOrder[orderId]
			isValid := order.Valid
			if isValid {
				order.Valid = false
			}

			utils.OutputOrderDeleted(request.activeOrder, isValid, GetCurrentTimestamp())
			close(request.doneChan)

		}
	}
}

func executeTransaction(activeOrder *utils.Input, restingOrder *Order) {
	transferCount := min(activeOrder.Count, restingOrder.Count)
	activeOrder.Count -= transferCount
	restingOrder.Count -= transferCount
	outputTime := GetCurrentTimestamp()
	utils.OutputOrderExecuted(restingOrder.OrderID, activeOrder.OrderId, restingOrder.ExecutionID, restingOrder.Price, transferCount, outputTime)
}

type Order struct {
	OrderID     uint32 // `uint32_t` in C/C++
	Price       uint32
	Count       uint32
	ExecutionID uint32
	Timestamp   uint32
	Valid       bool
}

type MinHeap []*Order

func (h MinHeap) Len() int      { return len(h) }
func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h MinHeap) Less(i, j int) bool {
	var o1, o2 *Order
	o1, o2 = h[i], h[j] // Go automatically dereference pointers!
	return (o1.Price < o2.Price) || (o1.Price == o2.Price && o1.Timestamp < o2.Timestamp)
}

func (h *MinHeap) Push(x any) {
	*h = append(*h, x.(*Order))
}

func (h *MinHeap) Pop() any {
	n := len(*h)
	res := (*h)[n-1]
	*h = (*h)[:n-1]
	return res
}

type MaxHeap []*Order

func (h MaxHeap) Len() int      { return len(h) }
func (h MaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h MaxHeap) Less(i, j int) bool {
	var o1, o2 *Order
	o1, o2 = h[i], h[j]
	return (o1.Price > o2.Price) || (o1.Price == o2.Price && o1.Timestamp < o2.Timestamp)
}

func (h *MaxHeap) Push(x any) {
	*h = append(*h, x.(*Order))
}

func (h *MaxHeap) Pop() any {
	n := len(*h)
	res := (*h)[n-1]
	*h = (*h)[:n-1]
	return res
}
