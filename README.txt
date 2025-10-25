
# data_gov_india.py — 405/404 Fix

**Why you saw 405:** You entered a full URL or `resource/<id>` into the **Resource ID** field.
The code then built: `.../resource/resource/<id>` → server replies **405 Method Not Allowed**.

**Fix:** This module sanitizes the Resource ID so you can paste **either**:
- just the UUID-like id (e.g., `9ef84268-d588-465a-a308-a864a43d0070`) **or**
- the full URL (e.g., `https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070`)

It will extract the correct ID and call the right endpoint.

**Correct request format example:**

```
https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070?api-key=YOUR_KEY&format=json&limit=1000
```
