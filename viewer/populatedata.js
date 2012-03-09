var neoLocation = 'http://localhost:7474/db/data'

//fetch stuff
function query(query) {
	$.post(neoLocation + '/cypher',
		{
			query : query,
			params : {}
		},
		processResponse).error(function(err) {
			if (err.responseText) {
				var data = JSON.parse(err.responseText)
				if (data.message && data.exception) {
					console.info('Query or request error!\nMessage: ' + data.message + '\nException: ' + data.exception)
					alert(data.message)
				}
			} else {
				alert("Error while requesting data! See log.")
				log.error(err)
			}
		})
}

function processResponse(data) {
	if (data.data) {
		console.info('Got ' + data.data.length + ' records.');
		populateWithInitial(data.data);
	} else {
		alert("Response has no data property. Don't know what to do!")
		console.warn('Got unrecognized response: ' + data)
	}
	
	populateMissing(function() {
		createVisualisationModel()
	})
}

function populateWithInitial(rows) {
	$.each(rows, function(rowindex, row) {
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
}

function populateMissing(callback) {
	var requests = []
	var contextRegex = new RegExp("^" + neoLocation + "(/.*)$")
	
	$.each(model.nodes, function(id, node) {
		if (typeof(node) == 'string') {
			requests.push({
				method: 'GET',
				to: groupFromRegex(contextRegex, node)
			})
		}
	})
	
	$.each(model.edges, function(start, edgeEnds) {
		$.each(edgeEnds, function(end, edge) {
			if (typeof(edge) == 'string') {
				requests.push({
					method: 'GET',
					to: groupFromRegex(contextRegex, edge)
				})
			}
		})
	})
	
	$.post(neoLocation + '/batch', JSON.stringify(requests), function(response) {
		processMissingResponse(response)
		callback()
	}).error(function(err) {
		if (err.responseText) {
			var data = JSON.parse(err.responseText)
			if (data.message && data.exception) {
				console.info('Query or request error!\nMessage: ' + data.message + '\nException: ' + data.exception)
				alert(data.message)
			}
		} else {
			alert("Error while requesting data! See log.")
			log.error(err)
		}
	})
}

function processMissingResponse(response) {
	$.each(response, function(key, batchResponseObject) {
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
	model.nodes[nodeId(node.self)] = node
}

function addNeoEdgeObject(edge) {
	var startId = nodeId(edge.start)
	var endId = nodeId(edge.end)
	
	if (!model.edges[startId]) {
		model.edges[startId] = {}
	}
	model.edges[startId][endId] = edge
	
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
		addEdgePlaceholder(startId, endId, path.relationships[i])
	}
	addNodePlaceholder(path.nodes[path.nodes.length - 1])
	
	model.paths.push(arr)
}

function addNeoTabularData(data) {
	model.tabular.push(data)
}

function addEdgePlaceholder(start, end, edge) {
	if (!model.edges[start]) {
		model.edges[start] = {}
	}
	
	if (!model.edges[start][end]) {
		model.edges[start][end] = edge
	}
}

function addNodePlaceholder(node) {
	var id = nodeId(node)
	if (!model.nodes[id]) {
		model.nodes[id] = node
	}
}

function neoObjectType(neoObject) {
	return neoObject.start && neoObject.end ? neoObject.type && neoObject.data ? 'EDGE' : 'PATH' : neoObject.data && neoObject.self ? 'NODE' : 'TABULAR'
}

function nodeId(input) {
	return groupFromRegex(new RegExp("^" + neoLocation + "/node/(\\d+)$"), input);
}

function edgeId(input) {
	return groupFromRegex(new RegExp("^" + neoLocation + "/relationship/(\\d+)$"), input);
}

function groupFromRegex(regex, input) {
	var groups = regex.exec(input)
	if (groups && groups.length == 2) {
		return groups[1]
	} else {
		return false
	}
}

function createVisualisationModel() {
	var viz = [];
	$.each(model.nodes, function(key, value) {
		var node = {
			id:key,
			name:value.data.name ? value.data.name.trim().replace('\n', '<br/>') : value.data.account,
			data:{
				properties: value.data
			},
			adjacencies:[]
		}
		if (model.edges[key]) {
			$.each(model.edges[key], function(edgeKey, edge) {
				node.adjacencies.push({'nodeTo': edgeKey, 'data' : {}})
			})
		}
		viz.push(node)
	})

	init(viz)
}
	// 
	// nodes:
	// {
	// 	'ID':{
	// 		node object stuff,
	// 		jitnode: jit-node-object
	// 	}
	// }
	// 
	// edges:
	// {
	// 	'12':{
	// 		'14':[{
	// 				edge object stuff,
	// 		 		jitedge: jit-edge-object
	// 			}]
	// 	}
	// 	'14':{
	// 		'12':[{ edge object }]
	// 	}
	// }
	// 
	// paths:
	// [
	// [[12,14], [14,18]],
	// [[12,14], [14,18]]
	// ]
	// 
	// onCreateLabel(dom, node) {
	// 	nodes[node.id]['jitnode'] = node
	// }
	// 
	
