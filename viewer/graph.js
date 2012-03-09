var model = {
	nodes:{},
	edges:{},
	paths:[],
	tabular:[]
}
var graphState = {}
var fd

var Log = {
    elem: false,
    write: function(text) {
        if (!this.elem)
        this.elem = document.getElementById('log')
        this.elem.innerHTML = text
    }
}

function init(json) {
    // init data
    fd = new $jit.ForceDirected({
        injectInto: 'infovis',
        Navigation: {
            enable: true,
            type: 'Native',
            panning: 'avoid nodes',
            zooming: 10
        },
        Node: {
            overridable: true,
            color: '#83548B',
            type: 'circle',
            dim: 7
        },
        Edge: {
            overridable: true,
            color: '#23A4FF',
            lineWidth: 1.0,
			type: 'line'
        },
        Events: {
            enable: true,
			enableForEdges: true,
            type: 'Native',
            onMouseEnter: function(node, eventInfo, event) {
				if (node && node.adjacencies) {
                	fd.canvas.getElement().style.cursor = 'move'
				} else if (node && node.nodeTo) {
                	fd.canvas.getElement().style.cursor = 'pointer'
				}
            },
            onMouseLeave: function() {
                fd.canvas.getElement().style.cursor = ''
            },
            //Update node positions when dragged
            onDragMove: function(node, eventInfo, e) {
				if (node && node.adjacencies) {
	                var pos = eventInfo.getPos()
	                node.pos.setc(pos.x, pos.y)
	                fd.plot()
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
				}
			}
        },
        //Number of iterations for the FD algorithm
        iterations: 400,
        //Edge length
        levelDistance: 180,
        // This method is only triggered
        // on label creation and only for DOM labels (not native canvas ones).
        onCreateLabel: createLabel,
        // Change node styles when DOM labels are placed
        // or moved.
        onPlaceLabel: placeLabel
    })
    // load JSON data.
    fd.loadJSON(json)
    // compute positions incrementally and animate.
    fd.computeIncremental({
        iter: 40,
        property: 'end',
        onStep: function(perc) {
            Log.write(perc + '% loaded...')
        },
        onComplete: function() {
            Log.write('done')
            fd.animate({
                modes: ['linear'],
                transition: $jit.Trans.Elastic.easeOut,
                duration: 2500
            })
        }
    })
    // end
}

function placeLabel(domElement, node) {
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

    nameContainer.className = 'name'
    nameContainer.innerHTML = node.name
    closeButton.className = 'close'
    closeButton.innerHTML = 'x'
    domElement.appendChild(nameContainer)
    domElement.appendChild(closeButton)
    //Fade the node and its connections when
    //clicking the close button
    closeButton.onclick = function() {
        node.setData('alpha', 0, 'end')
        node.eachAdjacency(function(adj) {
            adj.setData('alpha', 0, 'end')
        })
        fd.fx.animate({
            modes: ['node-property:alpha',
            'edge-property:alpha'],
            duration: 500
        })
    }
    //Toggle a node selection when clicking
    //its name. This is done by animating some
    //node styles like its dimension and the color
    //and lineWidth of its adjacencies.
    nameContainer.onclick = function() {
		if (graphState.selectedNode) {
            graphState.selectedNode.setData('dim', 7, 'end')
            graphState.selectedNode.eachAdjacency(function(adj) {
                adj.setDataset('end', {
                    lineWidth: 1.0,
                    color: '#23a4ff'
                })
            })
		}
		if (graphState.selectedNode == node) {
			graphState.selectedNode = undefined
		} else {
			graphState.selectedNode = node
            graphState.selectedNode.setData('dim', 17, 'end')
            graphState.selectedNode.eachAdjacency(function(adj) {
                adj.setDataset('end', {
                    lineWidth: 3,
                    color: '#36acfb'
                })
            })
		}				
        //trigger animation to final styles
        fd.fx.animate({
            modes: ['node-property:dim',
            'edge-property:lineWidth:color'],
            duration: 500
        })

        // Build the right column relations list.
        // This is done by traversing the clicked node connections.
        var html = "<h4>" + node.name + "</h4><b> properties:</b><ul>"
        var properties, edges = []

        $.each(node.data.properties, function(key, value) {
        	html = html + "<li>"+key +" = "+ value + "</li>"
        })
        
        html = html + "</ul><b> connections:</b><ul><li>"
        node.eachAdjacency(function(adj) {
            if (adj.getData('alpha')) edges.push(adj.nodeTo.name)
        })
        //append connections information
        $jit.id('inner-details').innerHTML = html + edges.join("</li><li>") + "</li></ul>"
    }
}