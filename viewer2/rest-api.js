function RestApi(location) {
	this.location = location
	var self = this
	
	function getCypherResult(query, callback) {
		$.post(location + '/cypher',
			{
				query : query,
				params : {}
			}, function(data) {
				if (data.data) {
					console.info('Got ' + data.data.length + ' records.');
					callback(data.data)
				} else {
					console.warn('Got unrecognized response.', data)
					callback(undefined, { message: 'Response has no data property!', exception: ''})
				}
			}).error(function(err) {
				console.error(err)
				if(err.responseText) {
					callback(undefined, JSON.parse(err.responseText))
				} else {
					callback(undefined, {message: 'Error while talking to REST API.'})
				}
			})
	}
	
	function getBatchResult(requests, callback) {
		$.post(location + '/batch', JSON.stringify(requests), function(response) {
			callback(response)
		}).error(function(err) {
			console.error(err)
			callback(undefined, err)
		})
	}


	this.resultForQuery = function(query, callback) {
		
	}
}