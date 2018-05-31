{%- extends 'full.tpl' -%}

{% block input_group -%}
{% endblock input_group %}

{%- block header -%}
{{ super() }}

<style type="text/css">
div.prompt {
    display: none;
}
.CodeMirror{
    font-family: "Consolas", sans-serif;
}
p {font-size:14px;}
</style>
{%- endblock header -%}