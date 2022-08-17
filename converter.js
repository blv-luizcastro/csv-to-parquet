const linkSchema = require('./schemas/links.js')
const accountSchema = require('./schemas/accounts.js')
const transactionShema = require('./schemas/transactions.js')
const ownersSchema = require('./schemas/owners.js')
const balanceSchema = require('./schemas/balances')

const convertCsvToParquet = async () => {
    try {
        await linkSchema()
        await accountSchema()
        await transactionShema()
        await ownersSchema()
        await balanceSchema()

    } catch (error) {
        console.error(error)
    }
}

convertCsvToParquet()