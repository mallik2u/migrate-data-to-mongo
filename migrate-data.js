const mongodb = require('mongodb')
const async = require('async')
const url = 'mongodb://localhost:27017'
const dbName = 'edx-course-db'
const collectionName = 'customers'
let customers = require('./m3-customer-data')
let addresses = require('./m3-customer-address-data')
let packetsize = parseInt(process.argv[2], 10) || customers.length
mongodb.MongoClient.connect(url, { useNewUrlParser: true }, (error, client) => {
    console.log('MongoDB connected')
    if (error) {
        console.error(error)
        return process.exit(1)
    }
    const db = client.db(dbName)
    let aTasks = []
    customers.forEach((customer, index) => {
        let start, limit
        customers[index] = Object.assign(customer, addresses[index])
        if (index % packetsize === 0) {
            start = index
            limit = start + packetsize
            limit = (limit > customers.length) ? customers.length : limit
            aTasks.push((callback) => {
                console.log(`Inserting records ${start} to ${limit} of ${customers.length}`)
                db.collection(collectionName).insertMany(customers.slice(start, limit), (error, results) => {
                    callback(error, results)
                })
            })
        }
    })
    console.log("Starting DB Migration");
    async.parallel(aTasks, (error, results) => {
        if (error) console.log(error)
        client.close(); console.log("Mongo DB Disconnected")
    })
})