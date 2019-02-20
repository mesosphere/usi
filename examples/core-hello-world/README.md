# Core Hello World Example Framework

Run the hello-world example framework that:
- relies on our `core` module to handle Mesos connection, event handling, offer matching etc. as
  opposed to the `simple-hello-world` example framework which uses only the Mesos client and has to 
  implement all of the above mentioned pieces itself
- starts one `echo "Hello, world" && sleep N` task
- exits should the task fail (or fail to start)