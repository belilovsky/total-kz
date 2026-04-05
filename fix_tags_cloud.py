#!/usr/bin/env python3
"""Fix tag cloud: show 25 tags + 'ещё' link."""

h = open('app/templates/public/home.html').read()

old = '{% for t in trending_tags[:15] %}'
new = '{% for t in trending_tags[:25] %}'
h = h.replace(old, new)

# Add "ещё" link after the tags loop
old_end = '''    {% endfor %}
  </div>
  {% endif %}

</div>
{% endblock %}'''

new_end = '''    {% endfor %}
    {% if trending_tags|length > 25 %}
    <a href="/search" class="trending-pill" style="background:var(--accent);color:#fff;">ещё →</a>
    {% endif %}
  </div>
  {% endif %}

</div>
{% endblock %}'''

h = h.replace(old_end, new_end, 1)
open('app/templates/public/home.html', 'w').write(h)
print("Tags: 15->25, added 'ещё' link")
