MYSQL_IMAGE := mysql:8.0
CONTAINER := pitrfx
PASSWORD := test
PORT := 33067
FIXTURE := mysql-8.0-row-full.bin

.PHONY: all clean

all: $(FIXTURE)

$(FIXTURE):
	docker run --rm -d --name $(CONTAINER) -e MYSQL_ROOT_PASSWORD=$(PASSWORD) -p $(PORT):3306 $(MYSQL_IMAGE) \
		--log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --server-id=1
	@echo "waiting for MySQL to start..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		docker exec $(CONTAINER) mysqladmin -uroot -p$(PASSWORD) ping 2>/dev/null && break; \
		sleep 3; \
	done
	docker cp setup.sql $(CONTAINER):/setup.sql
	docker exec $(CONTAINER) mysql -uroot -p$(PASSWORD) -e "SOURCE /setup.sql;"
	docker exec $(CONTAINER) mysql -uroot -p$(PASSWORD) -e "FLUSH LOGS;"
	docker cp $(CONTAINER):/var/lib/mysql/mysql-bin.000002 $(FIXTURE)
	docker stop $(CONTAINER)

clean:
	rm -f $(FIXTURE)
