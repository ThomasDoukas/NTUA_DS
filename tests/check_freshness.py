factor = ['3', '5']

print('\nWe have compared the results to the serial execution (all requests starting from a single node):')
print('------------------------------------------------------------------------------------------------')

for k in factor:
    eventual_mistakes = 0
    linearizability_mistakes = 0

    # serial: output when run by a single node
    with open('output/req/serial_linearizability_' + k + '.out', 'r') as f:
        serial = f.readlines()
    
    # not serial: output when each request starts from a random node
    with open('output/req/not_serial_linearizability_' + k + '.out', 'r') as f:
        linearizability = f.readlines()
    with open('output/req/not_serial_eventual_' + k + '.out', 'r') as f:
        eventual = f.readlines()

    # split insert -> len == 5
    # split not found -> len == 8

    for i, line in enumerate(serial):
        tokens = line.split(' ')
        # skip if it is an insert action 
        if len(tokens) == 5:
            continue
        else:
            if tokens[2] != linearizability[i].split(' ')[2]:
                linearizability_mistakes += 1
            if tokens[2] != eventual[i].split(' ')[2]:
                eventual_mistakes += 1

    print('Replication Factor k: ' + k)
    print('Eventual Consistency mistakes: ' + str(eventual_mistakes))
    print('Linearizability mistakes: ' + str(linearizability_mistakes))
    print('-------------------------------------')
