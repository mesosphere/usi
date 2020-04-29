# Keep-Alive Example Framework

Run the keep-alive example framework that:
- uses simplified Scheduler interface
- starts configurable amount of `echo "Hello, world" && sleep 20` tasks (default 100)
- keeps restarting the task if it finishes

# Deployment on DC/OS Enterprise

1. Launch strict cluster and setup the DC/OS CLI.
2. Create key pair:
   ```
   dcos security org service-accounts keypair usi.private.pem usi.pub.pem
   ```
3. Create user `usi`:
   ```
   dcos security org service-accounts create -p usi.pub.pem -d "For testing USI on strict" usi
   ```
4. Store private key as secret:
   ```
   dcos security secrets create -f ./usi.private.pem usi/private_key
   ```
5. Grant `usi` access:
   ```
   dcos security org users grant usi dcos:mesos:master:task:user:nobody create
   dcos security org users grant usi dcos:mesos:master:framework:role:usi read
   dcos security org users grant usi dcos:mesos:master:framework:role:usi create
   ```
6. Deploy the framework:
   ```
   dcos marathon app add keep-alive-framework-app.json
   ```
