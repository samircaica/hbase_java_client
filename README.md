# hbase_java_client

## Execution:
```

javac -cp `hbase classpath`:. HBaseTestConnectionK.java

java -cp `hbase classpath`:. HBaseTestConnectionK
```

If you have KrbException: Message stream modified (41) exception try commenting renew_lifetime fro krb5.conf
