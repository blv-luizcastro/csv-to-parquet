const parquet = require('parquetjs')
const csv = require('csv-parser')
const fs = require('fs')

const schema = new parquet.ParquetSchema({
    created_at: { type: 'UTF8', optional: true },
    transaction_id: { type: 'UTF8', optional: true },
    link_id: { type: 'UTF8', optional: true },
    account_id: { type: 'UTF8', optional: true },
    institution_name: { type: 'UTF8', optional: true },
    type: { type: 'UTF8', optional: true },
    amount: { type: 'UTF8', optional: true },
    status: { type: 'UTF8', optional: true },
    balance: { type: 'UTF8', optional: true },
    category: { type: 'UTF8', optional: true },
    currency: { type: 'UTF8', optional: true },
    gig_data: { type: 'JSON', optional: true },
    merchant: { type: 'JSON', optional: true },
    reference: { type: 'UTF8', optional: true },
    value_date: { type: 'UTF8', optional: true },
    description: { type: 'UTF8', optional: true },
    collected_at: { type: 'UTF8', optional: true },
    observations: { type: 'UTF8', optional: true },
    accounting_date: { type: 'UTF8', optional: true },
    internal_identification: { type: 'UTF8', optional: true }
})

module.exports = transactionShema = async () => {
    let writer = await parquet.ParquetWriter.openFile(schema, './files/parquet/transactions.parquet')
    let data = []

    fs.createReadStream('./files/csv/transactions.csv')
        .pipe(csv())
        .on('data', function (row) {
            data.push(row)
        })
        .on('end', async function () {

            for (const row of data) {
                await writer.appendRow({
                    created_at: row.created_at,
                    transaction_id: row.transaction_id,
                    link_id: row.link_id,
                    account_id: row.account_id,
                    institution_name: row.institution_name,
                    type: row.type,
                    amount: row.amount,
                    status: row.status,
                    balance: row.balance,
                    category: row.category,
                    currency: row.currency,
                    gig_data: row.gig_data,
                    merchant: row.merchant,
                    reference: row.reference,
                    value_date: row.value_date,
                    description: row.description,
                    collected_at: row.collected_at,
                    observations: row.observations,
                    accounting_date: row.accounting_date,
                    internal_identification: row.internal_identification
                })
            }

            await writer.close()
        })

}
