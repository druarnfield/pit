BEGIN TRANSACTION;

MERGE {{ .This }} AS target
USING (
{{ .SQL }}
) AS source
ON {{ range $i, $k := .UniqueKey }}{{ if $i }} AND {{ end }}target.[{{ $k }}] = source.[{{ $k }}]{{ end }}
WHEN MATCHED THEN
    UPDATE SET {{ range $i, $c := .UpdateColumns }}{{ if $i }}, {{ end }}target.[{{ $c }}] = source.[{{ $c }}]{{ end }}
WHEN NOT MATCHED THEN
    INSERT ({{ range $i, $c := .Columns }}{{ if $i }}, {{ end }}[{{ $c }}]{{ end }})
    VALUES ({{ range $i, $c := .Columns }}{{ if $i }}, {{ end }}source.[{{ $c }}]{{ end }});

COMMIT TRANSACTION;