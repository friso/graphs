function Graph(events, viewhelpers) {
	this.log = {
		elem: false,
		write: function(text) {
			if (!this.elem) {
				this.elem = document.getElementById('log')
			}
			this.elem.innerHTML = text
		}
	}
	
	this.viewhelpers = viewhelpers || {}
	if (!this.viewhelpers.widthForEdge) {
		this.viewhelpers.widthForEdge = function(edge) { return 1.0 }
	}
	if (!this.viewhelpers.colorForEdge) {
		this.viewhelpers.colorForEdge = function(edge) { return 'rgb(80, 80, 255)' }
	}
	if (!this.viewhelpers.colorForNode) {
		this.viewhelpers.colorForNode = function(node) { return 'orange' }
	}
	if (!this.viewhelpers.sizeForNode) {
		this.viewhelpers.sizeForNode = function(node) { return 8.0 }
	}
	if (!this.viewhelpers.typeForNode) {
		this.viewhelpers.typeForNode = function(node) { return 'circle' }
	}
	this.events = events || {}
	this.fd = new $jit.ForceDirected({
		injectInto: 'infovis',
		Navigation: {
			enable: true,
			type: 'Native',
			panning: 'avoid nodes',
			zooming: 10
		},
		Node: {
			overridable: true
		},
		Edge: {
			overridable: true,
			type: 'line'
		},
		Label: {
			type: 'HTML'
		},
		Events: {
			enable: true,
			enableForEdges: true,
			type: 'Native',
			onMouseWheel: function(node, eventInfo, event) {
				//at some point, track zoom level and change label size accordingly
			},
			onMouseEnter: function(node, eventInfo, event) {
				if (node && node.adjacencies) {
					self.fd.canvas.getElement().style.cursor = 'move'
				} else if (node && node.nodeTo) {
					self.fd.canvas.getElement().style.cursor = 'pointer'
				}
			},
			onMouseLeave: function() {
				self.fd.canvas.getElement().style.cursor = ''
			},
			//Update node positions when dragged
			onDragMove: function(node, eventInfo, e) {
				if (node && node.adjacencies) {
					var pos = eventInfo.getPos()
					node.pos.setc(pos.x, pos.y)
					self.fd.plot()
				}
			},
			//Implement the same handler for touchscreens
			onTouchMove: function(node, eventInfo, e) {
				$jit.util.event.stop(e)
				//stop default touchmove event
				this.onDragMove(node, eventInfo, e)
			},
			onClick: function(node, eventInfo, e) {
				if (node && node.nodeFrom) {
					edgeClicked(node, eventInfo, e)
				}
			}
		},
		iterations: 200,
		levelDistance: 170,
		onCreateLabel: createLabel,
		onPlaceLabel: placeLabel
	})
	
	var self = this
	
	function edgeClicked(edge) {
		if (self.events.edgeClicked) {
			self.events.edgeClicked(edge)
		}
	}
	
	function placeLabel(domElement) {
        var style = domElement.style
        var left = parseInt(style.left)
        var top = parseInt(style.top)
        var w = domElement.offsetWidth
        style.left = (left - w / 2) + 'px'
        style.top = (top + 10) + 'px'
        style.display = ''
	}
	
	function createLabel(domElement, node) {
		// Create a 'name' and 'close' buttons and add them
		// to the main node label
		var nameContainer = document.createElement('span')
		var closeButton = document.createElement('span')
		var addButton = document.createElement('span')
		var collapseButton = document.createElement('span')
		var useButton = document.createElement('span')

		nameContainer.className = 'name'
		nameContainer.innerHTML = node.name
		closeButton.className = 'close'
		closeButton.innerHTML = 'x'
		addButton.className = 'add'
		addButton.innerHTML = '+'
		collapseButton.className = 'collapse'
		collapseButton.innerHTML = '-'
		useButton.className = 'use'
		useButton.innerHTML = '^'

		domElement.appendChild(nameContainer)
		domElement.appendChild(closeButton)
		domElement.appendChild(addButton)
		domElement.appendChild(collapseButton)
		domElement.appendChild(useButton)

		closeButton.onclick = function() {
			if(self.events.closeButtonClicked) {
				self.events.closeButtonClicked(node)
			}
		}

		addButton.onclick = function() {
			if(self.events.addButtonClicked) {
				self.events.addButtonClicked(node)
			}
		}

		collapseButton.onclick = function() {
			if(self.events.collapseButtonClicked) {
				self.events.collapseButtonClicked(node)
			}
		}

		useButton.onclick = function() {
			if(self.events.useButtonClicked) {
				self.events.useButtonClicked(node)
			}
		}

		nameContainer.onclick = function() {
			if(self.events.nameClicked) {
				self.events.nameClicked(node)
			}
		}
	}
	
	this.updateModel = function(model) {
		var viz = [];
		$.each(model.nodes, function(key, value) {
			var node = {
				id: key,
				name: '<nobr>' + value.data.name + '</nobr>',
				data: {
					'$type': self.viewhelpers.typeForNode(value),
					'$color': self.viewhelpers.colorForNode(value),
					'$dim': self.viewhelpers.sizeForNode(value)
				},
				adjacencies: []
			}
			if (model.edges[key]) {
				$.each(model.edges[key], function(edgeKey, edge) {
					node.adjacencies.push({
						nodeTo: edgeKey,
						data: {
							'$lineWidth': self.viewhelpers.widthForEdge(edge),
							'$color': self.viewhelpers.colorForEdge(edge),
						}
					})
				})
			}
			viz.push(node)
		})
		
		if (viz.length > 0) {
			self.fd.loadJSON(viz)
			self.fd.computeIncremental({
				iter: 2,
				property: 'end',
				onStep: function(perc) {
					self.log.write('laying out ... '+ perc + '%')
				},
				onComplete: function() {
					self.log.write('done')
					self.fd.animate({
						modes: ['linear'],
						transition: $jit.Trans.Elastic.easeOut,
						duration: 2500
					})
				}
			})
		} else {
			self.log.write('done (nothing to add)')
		}
	}
	
	this.removeNodeById = function(nodeId) {
		var node = self.fd.graph.getNode(nodeId)
		node.setData('alpha', 0, 'end')
		node.eachAdjacency(function(adj) {
			adj.setData('alpha', 0, 'end')
		})
		self.fd.fx.animate({
			modes: ['node-property:alpha',
			'edge-property:alpha'],
			duration: 500,
			onComplete:function(){
				self.fd.graph.removeNode(nodeId)
				self.fd.labels.disposeLabel(nodeId)
			}
		})
	}
}
