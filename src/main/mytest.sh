cd ../mrapps && go build -buildmode=plugin crash.go
rm mr-out-*
cd ../main
go run mrcoordinator.go pg-being_ernest.txt pg-dorian_gray.txt&
go run mrworker.go ../mrapps/crash.so&
go run mrworker.go ../mrapps/crash.so&
rm /tmp/mr-*