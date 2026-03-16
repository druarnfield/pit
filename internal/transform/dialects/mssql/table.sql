BEGIN TRANSACTION;

IF OBJECT_ID('{{ .This }}', 'U') IS NOT NULL
    DROP TABLE {{ .This }};
{{ if .CTEBlock }}
{{ .CTEBlock }}
SELECT *
INTO {{ .This }}
FROM (
{{ .SelectSQL }}
) AS __pit_model;
{{ else }}
SELECT *
INTO {{ .This }}
FROM (
{{ .SQL }}
) AS __pit_model;
{{ end }}
COMMIT TRANSACTION;
