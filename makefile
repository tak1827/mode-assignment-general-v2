fmt:
	go fmt ./...

lint:
	go vet ./...

run:
	go run main.go $$START_TIME $$END_TIME

mesure:
	gtime -f "\nTime: %E\nMemory: %M KB" go run main.go $$START_TIME $$END_TIME debug

pprof:
	go tool pprof -http=:8080 ./your-binary mem.prof
