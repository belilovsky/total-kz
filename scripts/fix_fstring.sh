#!/bin/bash
# Fix missing f-prefix in robots_txt and llms_txt handlers
cd /opt/total-kz

sed -i 's/    content = """User-agent:/    content = f"""User-agent:/' app/public_routes.py
sed -i 's/    content = """# Total.kz/    content = f"""# Total.kz/' app/public_routes.py

echo "Patch applied. Checking:"
grep -n 'content = f"""' app/public_routes.py
