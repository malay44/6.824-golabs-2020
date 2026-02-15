#!/bin/bash

# Configurable parameters
N=3
TEST_NAME="2B"
DATE_TIME=$(date +%Y/%m/%d/%H:%M:%S:%N)
LOGS_DIR="test_logs/$DATE_TIME"

# Internal state
fail_count=0
run_count=1
failed_indices=()
break_loop=0

# Setup logs directory
mkdir -p "$LOGS_DIR"
echo "Running '$TEST_NAME' test $N times"
echo "Logs will be saved in: $LOGS_DIR"

# Signal handler
cleanup_on_signal() {
    echo "Received kill signal. Exiting."
    break_loop=1
}
trap cleanup_on_signal SIGINT SIGTERM

# Main loop
while [ $fail_count -lt $N ]; do
    if [ "$break_loop" -eq 1 ]; then
        break
    fi

    echo "=== Run $run_count ==="
    log_file="$LOGS_DIR/out_$run_count.txt"

    # Run test, save output
    GO111MODULE=off go test -run "$TEST_NAME" -race 2>&1 | tee "$log_file" | grep -E "(PASS|FAIL|race)"

    # Check for failure
    if grep -q "FAIL" "$log_file"; then
        new_fail_log="$LOGS_DIR/out_${run_count}_fail.txt"
        mv "$log_file" "$new_fail_log"
        failed_indices+=($run_count)
        fail_count=$((fail_count + 1))
        echo "Failure count: $fail_count/$N"
    fi

    run_count=$((run_count + 1))
done

trap - SIGINT SIGTERM

# Summary
if [ ${#failed_indices[@]} -gt 0 ]; then
    echo "Failed test indices: ${failed_indices[@]}"
else
    echo "No test runs failed."
fi
