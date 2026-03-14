BEGIN TRANSACTION;

DELETE FROM {{ .This }}
WHERE EXISTS (
    SELECT 1 FROM (
{{ .SQL }}
    ) AS __pit_source
    WHERE {{ range $i, $k := .UniqueKey }}{{ if $i }} AND {{ end }}{{ $.This }}.[{{ $k }}] = __pit_source.[{{ $k }}]{{ end }}
);

INSERT INTO {{ .This }}
{{ .SQL }};

COMMIT TRANSACTION;