const http = require('http')
const mongodb = require('mongodb')
const url = require('url')
const { MongoClient } = mongodb

const PORT = 3000
const mongoUrl = 'mongodb://localhost:27017/busRoutes'
let time = 0

http.createServer((req, res) => {
	time = new Date().getTime()
	let params = url.parse(req.url, true).query

	if (params !== null && typeof params.line !== 'undefined')
		getRoute(params.line).then((data) => {
			time = new Date().getTime() - time
			console.log('Request took ' + time + 'ms to finish')
			res.end(JSON.stringify(data))

		})

	else
		res.end('opa')

}).listen(PORT)

console.log("REST API listening on: http://localhost:%s", PORT)

function getRoute(trip) {
	return new Promise((resolve, reject) => {
		fetchItem('trips', { trip }).then((data) => {
			console.log('data', data);
			fetchItem('shapes', { id: data.shape }).then(resolve)
		})
	})
}

function fetchItem(collectionName, query) {
	return new Promise((resolve, reject) => {
		MongoClient.connect(mongoUrl, (err, db) => {
			if (!err) {
				let collection = db.collection(collectionName)

				collection.findOne(query).then((data, err) => {
					if (!err) 
						resolve(data)

					db.close()
				})

			} else {
				resolve()
				db.close()
			}
		})
	})
}