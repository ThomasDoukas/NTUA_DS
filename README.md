# Dingane
A distributed hash table implementation based on the Chord protocol.  
Distributed Systems, ECE-NTUA 2020-2021

## Included

* Python implementation using Flask and HTTP requests for sending and routing messages
* CLI tool
* Tests to measure system performance on given input
* Project report

## To run
Each node operates both as a server and as a client.

### Server

1. Start bootstrap node, providing necessary flags:
    * `python3 server.py -bs -p 5000` 
        * `-k repl_factor`, where repl_factor is a positive int, default = 1
        * `-c linearizability`, default = eventual
2. Start other nodes, only providing a valid port number:
    * `python3 server.py -p port`

### Client
To execute client actions one can use the CLI tool. 
When starting you only need to specify the port of a node already in the system, running on the same machine.
* `python3 cli.py -p port`

![GIF](https://github.com/phoevos/dingane/blob/main/9PQQ6yfWIG.gif)

## Tests

### Consecutive Writes  
![Consecutive Writes](https://github.com/phoevos/dingane/blob/main/tests/plots/write_thr.png)

### Consecutive Reads  
![Consecutive Reads](https://github.com/phoevos/dingane/blob/main/tests/plots/read_thr.png)

### Mixed Read-Write Requests  
![Mixed Read-Write Requests](https://github.com/phoevos/dingane/blob/main/tests/plots/mixed.png)

## Contributors

* [Phoevos Kalemkeris](https://github.com/phoevos)
* [Thomas Doukas](https://github.com/ThomasDoukas)
* [Giannis Psarras](https://github.com/giannispsarr)
