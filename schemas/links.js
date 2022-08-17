const parquet = require('parquetjs')
const csv = require('csv-parser')
const fs = require('fs')

const schema = new parquet.ParquetSchema({
    created_at: { type: 'UTF8', optional: true  },
    link_id: { type: 'UTF8', optional: true },
    access_mode: { type: 'UTF8', optional: true },
    status: { type: 'UTF8' },
    created_by: { type: 'UTF8', optional: true },
    external_id: { type: 'UTF8', optional: true },
    client_refresh: { type: 'UTF8', optional: true },
    institution_name: { type: 'UTF8', optional: true },
    institution_status: { type: 'UTF8', optional: true },
})

module.exports = linkSchema = async () => {
    let writer = await parquet.ParquetWriter.openFile(schema, './files/parquet/links.parquet')
    let data = []

    fs.createReadStream('./files/csv/links.csv')
    .pipe(csv())
        .on('data', function (row) {
            data.push(row)
        })
        .on('end', async function () {

            for (const row of data) {
                await writer.appendRow({
                    created_at: row.created_at,
                    created_at: row.created_at,
                    link_id: row.link_id,
                    institution_name: row.institution_name,
                    access_mode: row.access_mode,
                    status: row.status,
                    external_id: row.external_id
                })
            }
            await writer.close()
        })
}
