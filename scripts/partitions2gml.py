#Piece of python that takes a graph in the form produced by the MR job and creates GML out of it (readable by Cytoscape)
import fileinput
import sys

nodes = {}
edges = []
for line in fileinput.input(sys.argv[1]):
	parts = line.strip().split('\t')
	nodes[parts[1]] = parts[0]
	for node in parts[2].split(','):
		edges.append( (parts[1], node) )

print 'graph [\n\tcomment "Generated from adjaceny list file."\n\tdirected 1\n\tlabel "from: ' + sys.argv[1] + '"\n'

for node in nodes:
	print 'node [\n\tid ' + node + '\n\tlabel N' + node + '\n\tpartition ' + nodes[node] + '\n]\n'

for edge in edges:
	if (not edge[1] in nodes):
		nodes[edge[1]] = nodes[edge[0]]
		print 'node [\n\tid ' + edge[1] + '\n\tlabel N' + edge[1] + '\n\tpartition ' + nodes[edge[0]] + '\n]\n'
	print 'edge [\n\tsource ' + edge[0] + '\n\ttarget ' + edge[1] + '\n]\n'

print ']'
