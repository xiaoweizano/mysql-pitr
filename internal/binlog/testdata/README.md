# binlog testdata

`mysql-8.0-row-full.bin` 是从 MySQL 8.0 dump 出来的 binlog 文件，
包含 setup.sql 中的 INSERT/UPDATE/DELETE 操作。

## 重新生成

```
make -C testdata clean all
```

需要 docker。
