#!/bin/bash

# List of directories to check
directories=("tests") # Modify this list as needed

for dir in "${directories[@]}"; do
    for test_file in "$dir"/*.in; do
        echo "Running test: $test_file"

        output=$(./grader ./engine < "$test_file" 2>&1)

        if echo "$output" | grep -iq "test passed."; then
            if echo "$output" | grep -iq "Potential goroutine leak detected!"; then
                echo "Warning: Goroutine leak detected in test: $test_file"
                exit 1
            else
                echo "No error in test: $test_file"
            fi
        else
            echo "Error found in test: $test_file"
            exit 1
        fi

        echo "----------------------------------"
    done
done
