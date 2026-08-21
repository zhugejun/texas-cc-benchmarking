{#
    Creates a `raw_ipeds` schema of DuckDB views, one per IPEDS CSV in seeds/.

    This replaces the old Snowflake RAW_IPEDS tables: instead of loading the
    data into a warehouse, each view reads its CSV live via read_csv. The
    staging sources (_stg_sources.yml) point at these views, so the rest of
    the project is unchanged.

    nullstr handles IPEDS missing-value markers ('' and '.') so numeric
    columns are sniffed correctly and downstream ::numeric casts get NULL.
#}
{% macro create_raw_ipeds_views() %}
    {% set families = ['c_a', 'ef_d', 'effy', 'gr', 'hd', 'sfa'] %}
    {% set years = range(2020, 2025) %}

    {# Path is relative to the dbt project dir; run dbt from inside texas_cc_benchmarking/. #}
    {% set seeds_dir = 'seeds' %}

    create schema if not exists raw_ipeds;

    {% for family in families %}
        {% for year in years %}
            {% set table = family ~ '_' ~ year %}
            create or replace view raw_ipeds.{{ table }} as
                select * from read_csv(
                    '{{ seeds_dir }}/{{ table }}.csv',
                    header = true,
                    nullstr = ['', '.', 'NULL'],
                    sample_size = -1
                );
        {% endfor %}
    {% endfor %}
{% endmacro %}
