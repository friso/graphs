function Model(restApi) {
	this.restApi = restApi
	var self = this
	resetModel()
	
	this.populateFromQueryResult = function(result, callback) {
		resetModel()
		self.addQueryResult(result, callback)
	}
	
	this.addQueryResult = function(result, callback) {
		$.each(result, function(rowindex, row) {
			$.each(row, function(colindex, col) {
				switch(neoObjectType(col)) {
					case 'NODE':
						addNeoNodeObject(col)
						break;
					case 'EDGE':
						addNeoEdgeObject(col)
						break;
					case 'PATH':
						addNeoPathObject(col)
						break;
					case 'TABULAR':
						addNeoTabularData(col)
						break;
				}
			})
		})
		
		console.log(self.tabular)
		
		populateMissing(callback)
	}
	
	this.removeNodeById = function(id) {
		var removedNodes = []
		var candidates = {}
		$.each(self.edges, function(from, outgoing) {
			$.each(outgoing, function(to, edge) {
				if (to != id && from != id) {
					candidates[to] = candidates[to] ? candidates[to] + 1 : 1
					candidates[from] = candidates[from] ? candidates[from] + 1 : 1
				} else {
					candidates[to] = candidates[to] ? candidates[to] : 0
					candidates[from] = candidates[from] ? candidates[from] : 0
					delete(self.edges[from][to])
				}
			})
		})
		$.each(candidates, function(nodeId, count) {
			if (count == 0) {
				delete(self.nodes[nodeId])
				removedNodes.push(nodeId)
			}
		})
		
		return removedNodes
	}
	
	this.collapseNode = function(node) {
		var nodes = []
		$.each(node.adjacencies, function(id, adj) {
			var adjacentNode = adj.nodeFrom.id == node.id ? adj.nodeTo : adj.nodeFrom
			if (Object.keys(adjacentNode.adjacencies).length == 1) {
				delete(self.nodes[adjacentNode.id])
				if (self.edges[node.id] && self.edges[node.id][adjacentNode.id]) {
					delete(self.edges[node.id][adjacentNode.id])
				}
				if (self.edges[adjacentNode.id] && self.edges[adjacentNode.id][node.id]) {
					delete(self.edges[adjacentNode.id][node.id])
				}
				nodes.push(adjacentNode.id)
			}
		})
		
		return nodes
	}
	
	function populateMissing(callback) {
		var requests = []
		var contextRegex = new RegExp("^" + neoLocation + "(/.*)$")
	
		$.each(self.nodes, function(id, node) {
			if (typeof(node) == 'string') {
				requests.push({
					method: 'GET',
					to: groupFromRegex(contextRegex, node)
				})
			}
		})
	
		$.each(self.requiredEdges, function(edge, bogus) {
			requests.push({
				method: 'GET',
				to: groupFromRegex(contextRegex, edge)
			})
		})
		
		if (requests.length > 0) {
			self.restApi.getBatchResult(requests, function(missing, err) {
				if (err) {
					callback(err)
				} else {
					processMissing(missing)
					self.requiredEdges = {};
					callback()
				}
			})
		} else {
			callback()
		}
	}

	function processMissing(missing) {
		$.each(missing, function(key, batchResponseObject) {
			switch(neoObjectType(batchResponseObject.body)) {
				case 'NODE':
					addNeoNodeObject(batchResponseObject.body)
					break;
				case 'EDGE':
					addNeoEdgeObject(batchResponseObject.body)
					break;
			}
		})
	}

	function addNeoNodeObject(node) {
		self.nodes[nodeId(node.self)] = node
	}

	function addNeoEdgeObject(edge) {
		var startId = nodeId(edge.start)
		var endId = nodeId(edge.end)
	
		if (!self.edges[startId]) {
			self.edges[startId] = {}
		}
		self.edges[startId][endId] = edge
	
		addNodePlaceholder(edge.start)
		addNodePlaceholder(edge.end)
	}

	function addNeoPathObject(path) {
		arr = []
		for (var i = 0; i < path.nodes.length - 1; i++) {
			var startId = nodeId(path.nodes[i])
			var endId = nodeId(path.nodes[i+1])
		
			arr.push(startId, endId)
		
			addNodePlaceholder(path.nodes[i])
			addEdgePlaceholder(path.relationships[i])
		}
		addNodePlaceholder(path.nodes[path.nodes.length - 1])
	
		self.paths.push(arr)
	}

	function addNeoTabularData(data) {
		self.tabular.push(data)
	}

	function addEdgePlaceholder(edge) {
		self.requiredEdges[edge] = 1
	}

	function addNodePlaceholder(node) {
		var id = nodeId(node)
		if (!self.nodes[id]) {
			self.nodes[id] = node
		}
	}
	
	function neoObjectType(neoObject) {
		return neoObject == null ? 'TABULAR' : neoObject.start && neoObject.end ? neoObject.type && neoObject.data ? 'EDGE' : 'PATH' : neoObject.data && neoObject.self ? 'NODE' : 'TABULAR'
	}
	
	function resetModel() {
		self.nodes = {}
		self.edges = {}
		self.paths = []
		self.tabular = []
		self.requiredEdges = {}
	}
	
	function nodeId(input) {
		return groupFromRegex(new RegExp("^" + neoLocation + "/node/(\\d+)$"), input);
	}
	this.nodeId = nodeId

	function edgeId(input) {
		return groupFromRegex(new RegExp("^" + neoLocation + "/relationship/(\\d+)$"), input);
	}
	this.edgeId = edgeId

	function groupFromRegex(regex, input) {
		var groups = regex.exec(input)
		if (groups && groups.length == 2) {
			return groups[1]
		} else {
			return false
		}
	}
}