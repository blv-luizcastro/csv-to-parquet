const parquet = require('parquetjs')
const csv = require('csv-parser')
const fs = require('fs')

const schema = new parquet.ParquetSchema({
    created_at: { type: 'UTF8', optional: true },
    owner_id: { type: 'UTF8', optional: true },
    link_id: { type: 'UTF8', optional: true },
    collected_at: { type: 'UTF8', optional: true },
    internal_identification: { type: 'UTF8', optional: true },
    first_name: { type: 'UTF8', optional: true },
    last_name: { type: 'UTF8', optional: true },
    second_last_name: { type: 'UTF8', optional: true },
    business_name: { type: 'UTF8', optional: true },
    email: { type: 'UTF8', optional: true },
    phone_number: { type: 'UTF8', optional: true },
    address: { type: 'UTF8', optional: true },
    document_type: { type: 'JSON', optional: true },
    document_number: { type: 'JSON', optional: true }
})

module.exports = ownersSchema = async () => {
    let writer = await parquet.ParquetWriter.openFile(schema, './files/parquet/owners.parquet')
    let data = []

    fs.createReadStream('./files/csv/owners.csv')
        .pipe(csv())
        .on('data', function (row) {
            data.push(row)
        })
        .on('end', async function () {

            for (const row of data) {
                await writer.appendRow({
                    created_at: row.created_at,
                    owner_id: row.owner_id,
                    link_id: row.link_id,
                    collected_at: row.collected_at,
                    internal_identification: row.internal_identification,
                    first_name: row.first_name,
                    last_name: row.last_name,
                    second_last_name: row.second_last_name,
                    business_name: row.business_name,
                    email: row.email,
                    phone_number: row.phone_number,
                    address: row.address,
                    document_type: row.document_type,
                    document_number: row.document_number
                })
            }
            await writer.close()
        })

}
