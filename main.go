package main

import "fmt"

func main() {
	buf := []byte("hello")
	fmt.Println(buf[1])
	fmt.Println(buf[1:2])
}
