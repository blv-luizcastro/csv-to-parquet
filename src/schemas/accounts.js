const parquet = require('parquetjs')
const csv = require('csv-parser')
const fs = require('fs')

const schema = new parquet.ParquetSchema({
    created_at: { type: 'UTF8', optional: true },
    account_id: { type: 'UTF8', optional: true },
    link_id: { type: 'UTF8', optional: true },
    institution_name: { type: 'UTF8', optional: true },
    account_name: { type: 'UTF8', optional: true },
    account_type: { type: 'UTF8', optional: true },
    agency: { type: 'UTF8', optional: true },
    number: { type: 'UTF8', optional: true },
    balance_current: { type: 'UTF8', optional: true },
    balance_available: { type: 'UTF8', optional: true },
    category: { type: 'UTF8', optional: true },
    currency: { type: 'UTF8', optional: true },
    loan_data: { type: 'JSON', optional: true },
    credit_data: { type: 'JSON', optional: true },
    funds_data: { type: 'JSON', optional: true },
    gig_payment_data: { type: 'JSON', optional: true },
    balance_type: { type: 'UTF8', optional: true },
    internal_identification: { type: 'UTF8', optional: true },
    public_identification_name: { type: 'UTF8', optional: true },
    public_identification_value: { type: 'UTF8', optional: true },
})

module.exports = accountSchema = async () => {
    let writer = await parquet.ParquetWriter.openFile(schema, './files/parquet/accounts.parquet')
    let data = []

    fs.createReadStream('./files/csv/accounts.csv')
        .pipe(csv())
        .on('data', function (row) {
            data.push(row)
        })
        .on('end', async function () {

            for (const row of data) {
                await writer.appendRow({
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                    account_id: row.account_id,
                    link_id: row.link_id,
                    institution_name: row.institution_name,
                    account_name: row.account_name,
                    account_type: row.account_type,
                    agency: row.agency,
                    number: row.number,
                    balance_current: row.balance_current,
                    balance_available: row.balance_available,
                    category: row.category,
                    currency: row.currency,
                    loan_data: row.loan_data,
                    credit_data: row.credit_data,
                    funds_data: row.funds_data,
                    gig_payment_data: row.gig_payment_data,
                    balance_type: row.balance_type,
                    internal_identification: row.internal_identification,
                    public_identification_name: row.public_identification_name,
                    public_identification_value: row.public_identification_value

                })
            }
            await writer.close()
        })

}