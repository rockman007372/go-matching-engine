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
}

func (e *Engine) Init(ctx context.Context, wg *wg.WaitGroup) {
	e.wg = wg

	e.coordinatorChan = make(chan *InstrumentRequest)

	wg.Add(1)
	go coordinator(ctx, wg, e.coordinatorChan)
}

// Engine should shutdown once all goroutines shutdowns
func (e *Engine) Shutdown(ctx context.Context) {
	e.wg.Wait()

	// by this point, all go routines are closed
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
		handleConn(conn, ctx, e.coordinatorChan)
	}()
}

func handleConn(conn net.Conn, ctx context.Context, coordinatorChan chan *InstrumentRequest) {
	defer conn.Close()

	// client sends each input to the central goroutine through its outgoing channel.
	// The central goroutine sends back the relevant instrument through the incoming channel.
	replyChan := make(chan *Instrument)
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

			select {
			case cancelChan <- orderRequest:
			case <-ctx.Done():
				return
			}

		default:
			fmt.Fprintf(os.Stderr, "Got order: %c %v x %v @ %v ID: %v\n",
				in.OrderType, in.Instrument, in.Count, in.Price, in.OrderId)

			instrumentRequest := InstrumentRequest{
				input: in,
				replyChan: replyChan,
			}

			// poll to request service from central goroutine
			coordinatorChan <- &instrumentRequest

			// receive the instrument from the central goroutine
			instrument := <-instrumentRequest.replyChan

			// send the input to the corresponding instrument channel
			// then map id to that channel
			if in.OrderType == utils.InputBuy {
				select {
				case instrument.buyChan <- orderRequest:
				case <-ctx.Done():
					return
				}

				idToInstrument[in.OrderId] = instrument.cancelBuyChan
			} else {
				select {
				case instrument.sellChan <- orderRequest:
				case <-ctx.Done():
					return
				}

				idToInstrument[in.OrderId] = instrument.cancelSellChan
			}
		}

		<-orderRequest.doneChan // prevent client from sending the next request before the current request is processed

		// TODO: if context is cancelled, instrument goroutines may close. Can client get stuck when sending request?
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

			input := request.input

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

			request.replyChan <- instrument_map[input.Instrument]
		}
	}
}

// A request to the central coordinator to query for instrument
type InstrumentRequest struct {
	input utils.Input
	replyChan chan *Instrument
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

		start:
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

				select {
				case addSellRequest := <-addChan:
					// add to order book
					addSellOrder(addSellRequest.activeOrder, restingSellOrders, idToOrder)
					close(addSellRequest.doneChan)

					goto start // recheck again

				case addChan <- request:
				}

			} else {
				close(request.doneChan)
			}

		case request := <-addChan:

			addSellOrder(request.activeOrder, restingSellOrders, idToOrder)
			close(request.doneChan)

		case request := <-ins.cancelSellChan:
			cancelOrder(request.activeOrder, idToOrder)
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

		start:
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

				select {
				case addBuyRequest := <-addChan:
					// add to order book
					addBuyOrder(addBuyRequest.activeOrder, restingBuyOrders, idToOrder)
					close(addBuyRequest.doneChan)

					goto start // recheck again

				case addChan <- request:
				}

			} else {
				close(request.doneChan)
			}

		case request := <-addChan:
			addBuyOrder(request.activeOrder, restingBuyOrders, idToOrder)
			close(request.doneChan)

		case request := <-ins.cancelBuyChan:
			cancelOrder(request.activeOrder, idToOrder)
			close(request.doneChan)

		}
	}
}

func addBuyOrder(input utils.Input, restingBuyOrders *MaxHeap, idToOrder map[uint32]*Order) {
	timestamp := GetCurrentTimestamp()

	buyOrder := Order{
		OrderID:     input.OrderId,
		Price:       input.Price,
		Count:       input.Count,
		ExecutionID: 1,
		Timestamp:   timestamp,
		Valid:       true,
	}

	idToOrder[buyOrder.OrderID] = &buyOrder
	heap.Push(restingBuyOrders, &buyOrder)

	utils.OutputOrderAdded(input, timestamp)
}

func addSellOrder(input utils.Input, restingSellOrders *MinHeap, idToOrder map[uint32]*Order) {
	timestamp := GetCurrentTimestamp()

	sellOrder := Order{
		OrderID:     input.OrderId,
		Price:       input.Price,
		Count:       input.Count,
		ExecutionID: 1,
		Timestamp:   timestamp,
		Valid:       true,
	}

	idToOrder[sellOrder.OrderID] = &sellOrder
	heap.Push(restingSellOrders, &sellOrder)

	utils.OutputOrderAdded(input, timestamp)
}

func cancelOrder(input utils.Input, idToOrder map[uint32]*Order) {

	if order, ok := idToOrder[input.OrderId]; !ok {
		// This means the order was never added! it was fully matched earlier
		utils.OutputOrderDeleted(input, false, GetCurrentTimestamp())
	} else {
		// if order is valid, it has not been deleted
		isValid := order.Valid
		if isValid {
			order.Valid = false
		}

		utils.OutputOrderDeleted(input, isValid, GetCurrentTimestamp())
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
	Timestamp   int64
	Valid       bool
}

type MinHeap []*Order

func (h MinHeap) Len() int      { return len(h) }
func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h MinHeap) Less(i, j int) bool {
	if h[i].Price == h[j].Price {
		if h[i].Timestamp == h[j].Timestamp {
			return h[i].OrderID < h[j].OrderID
		}
		return h[i].Timestamp < h[j].Timestamp
	}
	return h[i].Price < h[j].Price
}

func (h *MinHeap) Push(x any) {
	*h = append(*h, x.(*Order))
}

func (h *MinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type MaxHeap []*Order

func (h MaxHeap) Len() int      { return len(h) }
func (h MaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h MaxHeap) Less(i, j int) bool {
	if h[i].Price == h[j].Price {
		if h[i].Timestamp == h[j].Timestamp {
			return h[i].OrderID < h[j].OrderID
		}
		return h[i].Timestamp < h[j].Timestamp
	}
	return h[i].Price > h[j].Price
}

func (h *MaxHeap) Push(x any) {
	*h = append(*h, x.(*Order))
}

func (h *MaxHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
