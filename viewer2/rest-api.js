function RestApi(location) {
	this.location = location
	var self = this
	
	function getCypherResult(query, callback) {
		$.post(self.location + '/cypher',
			{
				query : query,
				params : {}
			}, function(data) {
				if (data.data) {
					console.info('Got ' + data.data.length + ' records.');
					callback(data.data)
				} else {
					console.warn('Got unrecognized response.', data)
					callback(undefined, { message: 'Response has no data property!'} )
				}
			}).error(function(err) {
				console.error(err)
				if(err.responseText) {
					callback(undefined, JSON.parse(err.responseText))
				} else {
					callback(undefined, { message: 'Error while talking to REST API.'})
				}
			})
	}
	
	function getBatchResult(requests, callback) {
		$.post(self.location + '/batch', JSON.stringify(requests), function(response) {
			callback(response)
		}).error(function(err) {
			console.error(err)
			callback(undefined, err)
		})
	}

	function neoObjectType(neoObject) {
		return neoObject == null ? 'TABULAR' : neoObject.start && neoObject.end ? neoObject.type && neoObject.data ? 'EDGE' : 'PATH' : neoObject.data && neoObject.self ? 'NODE' : 'TABULAR'
	}

	function nodeId(input) {
		return groupFromRegex(new RegExp("^" + self.location + "/node/(\\d+)$"), input);
	}

	function edgeId(input) {
		return groupFromRegex(new RegExp("^" + self.location + "/relationship/(\\d+)$"), input);
	}

	function groupFromRegex(regex, input) {
		var groups = regex.exec(input)
		if (groups && groups.length == 2) {
			return groups[1]
		} else {
			return false
		}
	}

	function processCypherResult(result, callback) {
		var nodes = {}
		var edges = {}
		var requiredEdges = {}
		var paths = []
		var tabular = []
		var requests = []

		var contextRegex = new RegExp("^" + self.location + "(/.*)$")

		function addNeoNodeObject(node) {
			nodes[nodeId(node.self)] = node
		}

		function addNeoEdgeObject(edge) {
			var startId = nodeId(edge.start)
			var endId = nodeId(edge.end)
		
			if (!edges[startId]) {
				edges[startId] = {}
			}
			edges[startId][endId] = edge
		
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
		
			paths.push(arr)
		}

		function addNeoTabularData(data) {
			tabular.push(data)
		}

		function addEdgePlaceholder(edge) {
			requiredEdges[edge] = 1
		}

		function addNodePlaceholder(node) {
			var id = nodeId(node)
			if (!nodes[id]) {
				nodes[id] = node
			}
		}

		function createResultObject() {
			var result = {
				nodes: nodes,
				edges: edges
			}

			return result
		}

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

		$.each(nodes, function(id, node) {
			if (typeof(node) == 'string') {
				requests.push({
					method: 'GET',
					to: groupFromRegex(contextRegex, node)
				})
			}
		})
	
		$.each(requiredEdges, function(edge, bogus) {
			requests.push({
				method: 'GET',
				to: groupFromRegex(contextRegex, edge)
			})
		})
		
		if (requests.length > 0) {
			getBatchResult(requests, function(missing, err) {
				if (err) {
					callback(undefined, err)
				} else {
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

					callback(createResultObject())
				}
			})
		} else {
			callback(createResultObject())
		}


	}

	this.resultForQuery = function(query, callback) {
		getCypherResult(query, function(result, err) {
			if (err) {
				callback(undefined, err)
			} else {
				processCypherResult(result, callback)
			}
		})
	}
}