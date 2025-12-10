WITH (SELECT groupArray((toString(id), name)) FROM raw_coa.raw_coa__gls) AS id_name_map
SELECT
    gl.id,
    gl.tree,
    arrayStringConcat(
        arrayMap(
            x -> arrayFirst(y -> y.1 = x, id_name_map).2,
            splitByChar('.', gl.tree)
        ),
        ' > '
    ) AS label
FROM raw_coa.raw_coa__gls gl
