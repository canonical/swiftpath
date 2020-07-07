{{ fullname | escape | underline }}

.. rubric:: Description
.. automodule:: {{ fullname }}
.. currentmodule:: {{ fullname }}

{% if classes %}
.. rubric:: Classes
.. autosummary::
    :toctree:
    {% for class in classes %}
    {{ class }}
    {% endfor %}
{% endif %}

.. automodule:: {{ fullname }}
   :members:
   :undoc-members:
   :show-inheritance:
