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
3. Create user `strict-usi`:
   ```
   dcos security org service-accounts create -p usi.pub.pem -d "For testing USI on strict" strict-usi
   ```
4. Grant `strict-usi` access:
   ```
   curl -L -X PUT -k -H "Authorization: token=$(dcos config show core.dcos_acs_token)" \
       "$(dcos config show core.dcos_url)/acs/api/v1/acls/dcos:superuser/users/strict-usi/full"
   ```
5. Great a secret from `usi.private.pem`: `dcos ???`
6. Deploy the framework:
   ```
   dcos marathon app add keep-alive-framework-app.json
   ```
