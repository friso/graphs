function UI(restApi) {
	var self = this
	self.restApi = restApi

	function writeLog(message) {
		$('#log').text(message)
	}

	function handleError(err) {
		alert(err)
		writeLog('Query returned error!')
	}

	function addQuery(query) {
		writeLog('Running query...')
		self.restApi.resultForQuery(query, handleQueryResult)
	}

	function confirmHuge(result) {
		return (result.length < 70 || confirm('Add ' + result.length + ' records to the graph?'))
	}

	function handleQueryResult(result, err) {
		if (err) {
			console.error(err)
		} else {
			console.log(result)
		}
	}

	function setupQueryShowHide() {
		queryVisibleStyle = { width: '60%', height: '50%', opacity: 0.9}
		queryHiddenStyle = { width: '25%', height: '10%', opacity: 0.3}

		function hideQuery() {
			if (!$('#queryText').is(':focus')) {
				$('#query').animate(queryHiddenStyle, 100)
				$('#queryText').css({ overflow: 'hidden' })
			}
		}

		function showQuery() {
			$('#query').animate(queryVisibleStyle, 200)
			$('#queryText').css({ overflow: 'auto' })
		}
		$('#query').hover(showQuery, hideQuery)
		$('#query').focusout(hideQuery)
	}

	function setupLogShowHide() {
		logVisibleStyle = { opacity: 0.9}
		logHiddenStyle = { opacity: 0.3}

		function hideLog() {
			$('#log').animate(logHiddenStyle, 100)
		}

		function showLog() {
			$('#log').animate(logVisibleStyle, 200)
		}
		$('#log').hover(showLog, hideLog)
		$('#log').focusout(hideLog)
	}

	function setupQueryTextCtrlEnter() {
		$('#queryText').keypress(function(event) {
			var keyCode = (event.which ? event.which : event.keyCode);
			if (keyCode === 10 || keyCode == 13 && event.ctrlKey) {
				addQuery($('#queryText').val())
				return false
			}
		})
	}

	this.createUI = function() {

		setupQueryShowHide()
		setupLogShowHide()
		setupQueryTextCtrlEnter()

		makeGraph()
	}

	function makeGraph(graph) {
		var graph = Viva.Graph.graph();

		// Construct the graph
		graph.addNode('anvaka', {image_url : 'https://secure.gravatar.com/avatar/91bad8ceeec43ae303790f8fe238164b'});
		graph.addNode('manunt', {image_url : 'https://secure.gravatar.com/avatar/c81bfc2cf23958504617dd4fada3afa8'});
		graph.addNode('thlorenz', {image_url : 'https://secure.gravatar.com/avatar/1c9054d6242bffd5fd25ec652a2b79cc'});
		graph.addNode('bling', {image_url : 'https://secure.gravatar.com/avatar/24a5b6e62e9a486743a71e0a0a4f71af'});
		graph.addNode('diyan', {image_url : 'https://secure.gravatar.com/avatar/01bce7702975191fdc402565bd1045a8?'});
		graph.addNode('pocheptsov', {image_url : 'https://secure.gravatar.com/avatar/13da974fc9716b42f5d62e3c8056c718'});
		graph.addNode('dimapasko', {image_url : 'https://secure.gravatar.com/avatar/8e587a4232502a9f1ca14e2810e3c3dd'});

		// graph.addLink('anvaka', 'manunt');
		// graph.addLink('anvaka', 'thlorenz');
		graph.addLink('anvaka', 'bling');
		graph.addLink('bling', 'manunt');
		graph.addLink('bling', 'thlorenz');

		graph.addLink('anvaka', 'diyan');
		graph.addLink('anvaka', 'pocheptsov');
		graph.addLink('anvaka', 'dimapasko');

		// Set custom nodes appearance
		var graphics = Viva.Graph.View.svgGraphics();
		graphics.node(function(node) {
				if (node.data.image_url) {
					return Viva.Graph.svg('image')
						.attr('width', 24)
						.attr('height', 24)
						.link(node.data.image_url);
				} else if (node.data.name) {
					return Viva.Graph.svg('text')
						.text(node.data.name)
						.attr('style', 'text-transform: uppercase')
						.attr('font-size', '16px')
						.attr('text-anchor', 'middle')
						.attr('dominant-baseline', 'central')
				} else {
					return Viva.Graph.svg('circle').attr('r', 10)
				}
			})
			.placeNode(function(nodeUI, pos){
				if (nodeUI.tagName == 'image') {
					nodeUI.attr('x', pos.x - nodeUI.attr('width') / 2).attr('y', pos.y - nodeUI.attr('height') / 2);
				} else if (nodeUI.tagName == 'circle') {
					nodeUI.attr('cx', pos.x).attr('cy', pos.y);
				} else {
					nodeUI.attr('x', pos.x).attr('y', pos.y);
				}
			});

		var layout = Viva.Graph.Layout.forceDirected(graph, {
			springLength : 100,
			springCoeff : 0.0005,
			dragCoeff : 0.015,
			gravity : -1.2
		});


		var renderer = Viva.Graph.View.renderer(graph, 
			{
				graphics : graphics,
				layout : layout
			});

		self.graph = graph;

		renderer.run();
	}
}