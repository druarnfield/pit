BEGIN TRANSACTION;
{{ if .CTEBlock }}
{{ .CTEBlock }}
{{ end }}
DELETE FROM {{ .This }}
WHERE EXISTS (
    SELECT 1 FROM (
{{ .SelectSQL }}
    ) AS __pit_source
    WHERE {{ range $i, $k := .UniqueKey }}{{ if $i }} AND {{ end }}{{ $.This }}.[{{ $k }}] = __pit_source.[{{ $k }}]{{ end }}
);
{{ if .CTEBlock }}
{{ .CTEBlock }}
{{ end }}
INSERT INTO {{ .This }}
{{ .SelectSQL }};

COMMIT TRANSACTION;
