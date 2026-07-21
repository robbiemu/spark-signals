# NATS credentials and subject permissions

The deployed single-host profile uses generated username/password credentials
and binds NATS to loopback. The agent may publish only
`spark.v1.example.spark-node-01.>` and cannot subscribe. The bridge may subscribe to
`spark.v1.>` and cannot publish. See `deploy/nats/nats-server.conf`.

Both binaries also accept a NATS `.creds` file through `NATS_CREDENTIALS` or
`--nats-credentials`. A credentials file combines a user JWT with its private
user NKey seed; it must be treated as a secret, stored outside the repository,
and mode `0600`. This is the intended multi-host upgrade path.

One possible `nsc` bootstrap is:

```console
nsc add operator --name spark-signals
nsc add account --name telemetry
nsc add user --name spark-node-01-agent \
  --allow-pub 'spark.v1.example.spark-node-01.>' --deny-sub '>'
nsc add user --name otel-bridge \
  --allow-sub 'spark.v1.>' --deny-pub '>'
nsc generate config --nats-resolver > resolver.conf
```

`nsc add user` writes each user credential file beneath its configured NKeys
directory. Set `NATS_CREDENTIALS` in the corresponding runtime environment file
and remove `NATS_USER`/`NATS_PASSWORD`. The existing Rust client path loads the
JWT and seed from the credentials file and supports the same reconnect replay.

For a production operator, use account/operator signing keys, back up the JWT
store separately from the private NKeys store, rotate users independently, and
distribute only each node's credential file. The server resolver configuration
and account JWT deployment should be tested in a staging security domain before
replacing the loopback profile.

For the Dockerized username/password profile, set
`SPARK_AGENT_PUBLISH_SUBJECT` in ignored `deploy/runtime/nats.env` to the exact
site/node prefix the agent is allowed to publish, including the terminal `>`.

Client TLS is independent of the identity mechanism. `NATS_CA` or `--nats-ca`
adds a private CA and requires TLS. A remotely reachable broker should require
TLS and deliberate firewall exposure; the repository example remains loopback
only.
