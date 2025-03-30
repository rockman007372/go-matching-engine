#!/bin/bash

# List of directories to check
directories=("tests" "tests/32_threads" "tests/64_threads" "tests/ultimate" ) # Modify this list as needed
# directories=("tests/64_threads") # Modify this list as needed
# directories=("tests/ultimate") # Modify this list as needed

# Log file to save the output in case of an error
log_file="error_log.txt"

# Clear previous log file content if it exists
> "$log_file"

for dir in "${directories[@]}"; do
    for test_file in "$dir"/*.in; do
        echo "Running test: $test_file ..."

        output=$(./grader ./engine < "$test_file" 2>&1)

        if echo "$output" | grep -iq "test passed."; then
            if echo "$output" | grep -iq "Potential goroutine leak detected!"; then
                echo "Warning: Goroutine leak detected."
                exit 1
            else
                echo "Test passed."
            fi
        else
            echo "ERROR!!!"

            # Write the entire output to the log file
            echo "$output" >> "$log_file"

             # Get the last line of the output
            last_line=$(echo "$output" | tail -n 1)
            
            # Print the last line of the output
            echo "$last_line"
            
            exit 1
        fi

        echo "----------------------------------"
    done
done

# for i in {1..10}; do
#     echo "Running test iteration: $i"
#     for dir in "${directories[@]}"; do
#         for test_file in "$dir"/*.in; do
#             echo "Running test: $test_file ..."

#             output=$(./grader ./engine < "$test_file" 2>&1)

#             if echo "$output" | grep -iq "test passed."; then
#                 if echo "$output" | grep -iq "Potential goroutine leak detected!"; then
#                     echo "Warning: Goroutine leak detected."
#                     exit 1
#                 else
#                     echo "Test passed."
#                 fi
#             else
#                 echo "ERROR!!!"

#                 # Write the entire output to the log file
#                 echo "$output" >> "$log_file"

#                 # Get the last line of the output
#                 last_line=$(echo "$output" | tail -n 1)
                
#                 # Print the last line of the output
#                 echo "$last_line"
                
#                 exit 1
#             fi

#             echo "----------------------------------"
#         done
#     done
# done