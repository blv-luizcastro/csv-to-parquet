const linkSchema = require('./schemas/links.js')
const accountSchema = require('./schemas/accounts.js')
const transactionShema = require('./schemas/transactions.js')
const ownersSchema = require('./schemas/owners.js')

const convertCsvToParquet = async () => {
    try {
        await linkSchema()
        await accountSchema()
        await ownersSchema()
        await transactionShema()
    } catch (error) {
        console.error(error)
    }
}

convertCsvToParquet()