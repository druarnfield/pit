BEGIN TRANSACTION;

IF OBJECT_ID('{{ .This }}', 'U') IS NOT NULL
    DROP TABLE {{ .This }};

SELECT *
INTO {{ .This }}
FROM (
{{ .SQL }}
) AS __pit_model;

COMMIT TRANSACTION;