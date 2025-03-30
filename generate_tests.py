import os
import random

thread_no = 64
instruments = [str(i) for i in range(10)]
price_lower, price_uppper = 100, 200
quantity_lower, quantity_upper = 10, 1000

for i in range(100): 
    dir_name = f"tests/{thread_no}_threads"
    file_name = f"{dir_name}/sample_{str(i)}.in"

    # Ensure the directory exists
    os.makedirs(dir_name, exist_ok=True)

    # Create or clear file then close
    with open(file_name, 'w') as f:
        pass  # Just to clear the file

    # Open in append mode
    with open(file_name, 'a') as f:
        f.write(f"{thread_no}\n")
        f.write("o\n")
        
        id = 100
        order_map = [[] for _ in range(thread_no)]  # Avoid using 'map' as variable name
        
        for _ in range(10000):
            id += 1
            op = random.randint(1,3)
            client = random.randint(0, thread_no - 1)
            if op == 1:
                f.write(f"{client} B {id} {random.choice(instruments)} {random.randint(price_lower, price_uppper)} {random.randint(quantity_lower, quantity_upper)}\n")
                order_map[client].append(id)

            elif op == 2:
                f.write(f"{client} S {id} {random.choice(instruments)} {random.randint(price_lower, price_uppper)} {random.randint(quantity_lower, quantity_upper)}\n")
                order_map[client].append(id)

            elif op == 3:
                if order_map[client]:
                    cancelId = random.choice(order_map[client])
                    f.write(f"{client} C {cancelId}\n")
                    
        f.write("x\n")
