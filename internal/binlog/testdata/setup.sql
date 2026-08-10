CREATE DATABASE IF NOT EXISTS shop;
USE shop;
CREATE TABLE orders (
    id BIGINT NOT NULL AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'new',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

INSERT INTO orders (user_id, amount, status) VALUES (1, 19.99, 'new'), (2, 50.00, 'paid');
UPDATE orders SET status = 'paid' WHERE id = 1;
DELETE FROM orders WHERE id = 2;
