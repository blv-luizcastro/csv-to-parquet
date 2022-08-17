const parquet = require('parquetjs')
const csv = require('csv-parser')
const fs = require('fs')

const schema = new parquet.ParquetSchema({
    balance_id: { type: 'UTF8', optional: true },
    account_id: { type: 'UTF8', optional: true },
    value_date: { type: 'UTF8', optional: true },
    balance: { type: 'UTF8', optional: true },
    collected_at: { type: 'UTF8', optional: true }
})

module.exports = linkSchema = async () => {
    let writer = await parquet.ParquetWriter.openFile(schema, './files/parquet/balances.parquet')
    let data = []

    fs.createReadStream('./files/csv/balances.csv')
        .pipe(csv())
        .on('data', function (row) {
            data.push(row)
        })
        .on('end', async function () {

            for (const row of data) {
                await writer.appendRow({
                    balance_id: row.created_at,
                    account_id: row.created_at,
                    value_date: row.link_id,
                    balance: row.institution_name,
                    collected_at: row.access_mode
                })
            }
            await writer.close()
        })
}
