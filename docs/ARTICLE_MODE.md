# Article Mode for Statistics Dashboard

## Overview

The statistics dashboard now supports an optional **article mode** that allows you to add narrative text and context around your charts. When `docs/article.md` exists, the dashboard becomes a full research report with prose between visualizations. When it doesn't exist, the dashboard looks exactly as before (normal mode).

## Quick Start

1. **Install dependencies** (if not already installed):
   ```bash
   pip install mistune>=3.0.0
   ```

2. **Create your article** at `docs/article.md`:
   ```bash
   cp docs/article.md.example docs/article.md
   # Edit docs/article.md with your narrative
   ```

3. **Generate the dashboard**:
   ```bash
   python scripts/generate_stats.py
   ```

4. **View the result**:
   - Open `docs/index.html` in your browser
   - The dashboard will now include your narrative text

## How It Works

### Normal Mode (No article.md)
- Dashboard displays all charts in predefined sections
- Standard layout with data collection metrics and analysis

### Article Mode (With article.md)
- Dashboard displays your custom narrative
- Charts appear where you specify using markers
- Only referenced charts are shown
- Full Markdown formatting support

## Markdown Markers

### Insert Charts
```markdown
<!-- chart: chart_id -->
```

Available chart IDs:
- `daily_flow` - Daily search volume by client (raw)
- `client_distribution` - Geographic distribution (raw)
- `client_convergence` - Search distribution across clients (shows convergence)
- `daily_unique_users` - Daily active users trend
- `top_queries` - Most searched queries
- `query_patterns` - Query structure analysis
- `query_length` - Query length distribution
- `temporal_hourly` - Searches by hour of day
- `ngrams` - Common 2-word and 3-word phrases
- `cooccurrence` - Term co-occurrence network
- `user_activity` - Most active users

### Insert Stats Cards
```markdown
<!-- stats-grid -->
```

This inserts the summary statistics cards (Total Searches, Unique Users, etc.)

## Markdown Formatting

Full Markdown support includes:

- **Headers**: `# H1`, `## H2`, `### H3`
- **Emphasis**: `**bold**`, `*italic*`
- **Lists**: Unordered (`-`) and ordered (`1.`)
- **Links**: `[text](url)`
- **Code**: `` `inline` `` and ` ```blocks``` `
- **Blockquotes**: `> quote`

## Example Article Structure

```markdown
# My Research Report

Introduction paragraph explaining the research...

<!-- stats-grid -->

## Methodology

Explanation of data collection approach...

<!-- chart: daily_flow -->

The chart above shows...

### Regional Analysis

<!-- chart: client_distribution -->

## Results

### Search Patterns

<!-- chart: top_queries -->

Analysis of top queries reveals...

<!-- chart: query_patterns -->

## Conclusion

Summary and findings...
```

## Styling

The article mode includes optimized styling for:

- **Prose**: Max-width 800px, comfortable line height (1.7)
- **Headers**: H2 and H3 with appropriate spacing
- **Code blocks**: Syntax highlighting with grey background
- **Charts**: Centered, max-width 1200px
- **Lists**: Proper indentation and spacing
- **Blockquotes**: Left border accent

## Tips

1. **Start simple**: Use the example file and modify incrementally
2. **Test locally**: Generate frequently to see your changes
3. **Chart IDs**: Reference the list above for exact chart identifiers
4. **Missing charts**: If you reference a non-existent chart ID, a warning appears but rendering continues
5. **Order control**: Only charts you reference appear, in the order you specify

## Switching Between Modes

- **Enable article mode**: Create/rename `docs/article.md`
- **Disable article mode**: Delete/rename `docs/article.md`

The script automatically detects which mode to use.

## Troubleshooting

### Chart not appearing
- Check the chart ID spelling (case-sensitive)
- Ensure the marker syntax is exact: `<!-- chart: chart_id -->`
- Look for warnings in script output

### Formatting looks wrong
- Ensure proper Markdown syntax (check with a Markdown preview tool)
- Check for unclosed tags or malformed markers

### No content showing
- Verify `docs/article.md` exists and has content
- Check console output for "Article mode" confirmation
- Ensure at least some content is present between markers

## Advanced Usage

### Custom Sections

You can organize content into logical sections:

```markdown
## Overview
<!-- stats-grid -->

## Data Collection
<!-- chart: daily_flow -->
<!-- chart: client_distribution -->

## Analysis
<!-- chart: top_queries -->
<!-- chart: query_patterns -->
```

### Contextual Narrative

Provide context before and after charts:

```markdown
### Search Activity

The network shows distinct daily patterns:

<!-- chart: temporal_hourly -->

Peak activity occurs during evening hours (18:00-22:00 UTC),
suggesting global user concentration in specific timezones.
```

### Multi-Level Headers

Use H2 for main sections and H3 for subsections:

```markdown
## Search Behavior Analysis

### Query Patterns

<!-- chart: query_patterns -->

### Temporal Trends

<!-- chart: temporal_hourly -->
```

## Example Files

- `docs/article.md.example` - Complete example showing all chart types
- Copy and customize for your needs

## Questions?

See the main project README or examine `scripts/generate_stats.py` for implementation details.
