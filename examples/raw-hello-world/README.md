# Raw Hello World Example Framework

Run a mesos-client example framework that:  
- uses only the raw mesos-client and none of the core scheduling logic
- successfully subscribes to Mesos master  
- starts one `echo "Hello, world" && sleep N` task  
- exits should the task fails (or fails to start)
 
Good to test against local Mesos. Note that we neither suggest, nor support, using the mesos-client on its own without the other components within this repository.
