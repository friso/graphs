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

print '''
graph: {title: "graph"
	layoutalgorithm: tree
	scaling        : 2.0
	colorentry 42  : 152 222 255
	node.shape     : ellipse
	node.color     : 42
	node.height    : 32
	node.fontname  : "helvB08"
	edge.color     : darkred 
	edge.arrowsize :  6
	node.textcolor : darkblue
	splines        : yes
'''


for node in nodes:
	print 'node: { title: "' + node + '" }'

for edge in edges:
	if (not edge[1] in nodes):
		nodes[edge[1]] = nodes[edge[0]]
		print 'node: { title: "' + edge[1] + '" }'
	
	print 'edge: { source: "' + edge[0] + '" target: "' + edge[1] + '"}'

print '}'
