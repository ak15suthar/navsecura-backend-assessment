.PHONY: up down install migrate dev worker test build
up:
	docker compose up -d postgres

down:
	docker compose down

install:
	npm install

migrate:
	npm run migrate

dev:
	npm run dev

worker:
	npm run worker:dev

test:
	npm test

build:
	npm run build
