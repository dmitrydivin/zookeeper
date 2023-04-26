# Zookeeper on the Tarantool
Supported the following commands

- `get <options> <path>` - get value by path
- `set <options> <path> <value>` - set value by path
- `exists <options> <path>` - check the value by path exists
- `del <options> <path>` - delete existing value by path
- `sub <options> <path>` - subscribe to the path when the value will update
- `unsub <options> <path>` - remove the subscription from the path
- `exit` - close socket connection on the server side

where is options
- `--routing-key=<value>` - routing key matcher for the client

You can start master-master replica set with the following command
```sh
docker-compose up
```

Then you can set value on the first master replica

```sh
> nc 127.0.0.1 3311
{"name":"tarantool-vs-zookeeper","version":"1.0"}
> set /alice/lock new-value
{"latency":3,"command":"set","last_modified":1682502915785,"value":"new-value","key":"\/alice\/lock","updated_count":2}
> exit
```

Then you can get the value on the second master replica
```sh
> nc 127.0.0.1 3312
{"name":"tarantool-vs-zookeeper","version":"1.0"}
> get /alice/lock
{"latency":0,"last_modified":1682502915785,"value":"new-value","key":"\/alice\/lock","updated_count":2}
> exit
```


