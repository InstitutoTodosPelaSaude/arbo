{% macro fix_brazilian_state(column) %}
    CASE 
        WHEN {{column}} IN (
            'ACRE', 'ALAGOAS', 'AMAPA', 'AMAZONAS', 'BAHIA', 'CEARA', 
            'DISTRITO FEDERAL', 'ESPIRITO SANTO', 'GOIAS', 'MARANHAO', 
            'MATO GROSSO', 'MATO GROSSO DO SUL', 'MINAS GERAIS', 'PARA', 
            'PARAIBA', 'PARANA', 'PERNAMBUCO', 'PIAUI', 'RIO DE JANEIRO', 
            'RIO GRANDE DO NORTE', 'RIO GRANDE DO SUL', 'RONDONIA', 'RORAIMA', 
            'SANTA CATARINA', 'SAO PAULO', 'SERGIPE', 'TOCANTINS'
        ) THEN {{column}}
        ELSE 'NOT_REPORTED'
    END 
{% endmacro %}