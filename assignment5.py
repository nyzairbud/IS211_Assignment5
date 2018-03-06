import argparse
import csv


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, help='Data file to process.')
    parser.add_argument('--servers', type=int, help='Number of Servers.')
    args = parser.parse_args()

    if args.file:
        try:
            """If there is a file, see if there are # of servers """
            numServers = args.servers
            if (numServers > 1):
                get_file = simulateManyServers(args.file, numServers)
            else:
                get_file = simulateOneServer(args.file)
        except:
            print 'Invalid'
    else:
        print 'Please enter another filename or URL.'


class Queue:

    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)


class Server:

    def __init__(self):
        self.current_task = None
        self.time_remaining = 0

    def tick(self):
        if self.current_task != None:
            self.time_remaining = self.time_remaining - 1
            if self.time_remaining <= 0:
                self.current_task = None

    def busy(self):
        if self.current_task != None:
            return True
        else:
            return False

    def start_next(self, new_task):
        self.current_task = new_task
        self.time_remaining = new_task.get_length()


class Request:

    def __init__(self, time, length):
        self.timestamp = time
        self.length = int(length)

    def get_stamp(self):
        return self.timestamp

    def get_length(self):
        return self.length

    def wait_time(self, cur_time):
        return cur_time - self.timestamp


def simulateOneServer(file):

    server = Server()

    """ Create a queue """
    queue = Queue()
    waiting_times = []

    with open(file, 'rb') as csvfile:
        response = csv.reader(csvfile, delimiter=',')

        for row in response:
            timestamp = int(row[0])
            file = row[1]
            length = int(row[2])
            req = Request(timestamp, length)
            queue.enqueue(req)

            if (not server.busy()) and (not queue.is_empty()):
                next_request = queue.dequeue()
                waiting_times.append(next_request.wait_time(timestamp))
                server.start_next(req)

            server.tick()

    average_wait = sum(waiting_times) / len(waiting_times)
    print("Average Wait %6.2f secs %3d tasks remaining." % (average_wait, queue.size()))


def simulateManyServers(file, numServers):

    servers = []
    for serverNum in range(0, numServers):
        servers.append(Server())

    queues = []
    """ Create many queues """
    for serverNum in range(0, numServers):
        queues.append(Queue())

    waiting_times = []
    """ Create many waiting times """
    for serverNum in range(0, numServers):
        waiting_times.append([])

    with open(file, 'rb') as csvfile:
        response = csv.reader(csvfile, delimiter=',')

        roundRobinPosition = 0

        for row in response:
            timestamp = int(row[0])
            file = row[1]
            length = int(row[2])
            req = Request(timestamp, length)

            queues[roundRobinPosition].enqueue(req)
            if roundRobinPosition < numServers - 1:
                roundRobinPosition += 1
            else:
                roundRobinPosition = 0

            if (not servers[roundRobinPosition].busy()) and (not queues[roundRobinPosition].is_empty()):
                next_request = queues[roundRobinPosition].dequeue()
                waiting_times[roundRobinPosition].append(next_request.wait_time(timestamp))
                servers[roundRobinPosition].start_next(req)

            servers[roundRobinPosition].tick()

        for serverNum in range(0, numServers):
            average_wait = sum(waiting_times[serverNum]) / len(waiting_times[serverNum])
            print("Average Wait %6.2f secs %3d tasks remaining." % (average_wait, queues[serverNum].size()))


if __name__ == '__main__':
    url = 'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'
    main()