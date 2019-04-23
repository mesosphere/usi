# Keep-Alive Example Framework

Run the keep-alive example framework that:
- uses simplified Scheduler interface
- starts configurable amount of `echo "Hello, world" && sleep 20` tasks (default 100)
- keeps restarting the task if it finishes