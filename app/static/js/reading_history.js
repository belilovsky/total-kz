/**
 * Reading history tracker for total.kz
 * Stores reading patterns in localStorage for personalized recommendations.
 */
(function() {
  'use strict';

  var STORAGE_KEY = 'total_reading_history';
  var MAX_ITEMS = 50;

  function getHistory() {
    try {
      var raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return { articles: [] };
      var data = JSON.parse(raw);
      if (!data || !Array.isArray(data.articles)) return { articles: [] };
      return data;
    } catch (e) {
      return { articles: [] };
    }
  }

  function saveHistory(data) {
    try {
      // Keep only last MAX_ITEMS
      if (data.articles.length > MAX_ITEMS) {
        data.articles = data.articles.slice(-MAX_ITEMS);
      }
      localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    } catch (e) {
      // localStorage full or disabled — ignore
    }
  }

  /**
   * Record that the user read an article.
   * @param {number} articleId
   * @param {string} category - sub_category slug
   * @param {number[]} entityIds - entity IDs mentioned in article
   */
  function trackArticle(articleId, category, entityIds) {
    if (!articleId) return;
    var history = getHistory();
    // Deduplicate: remove old entry for same article
    history.articles = history.articles.filter(function(a) {
      return a.id !== articleId;
    });
    history.articles.push({
      id: articleId,
      category: category || '',
      entities: (entityIds || []).slice(0, 10),
      ts: Date.now()
    });
    saveHistory(history);
  }

  /**
   * Get user's top reading preferences.
   * Returns {categories: string[], entities: number[], exclude: number[]}
   */
  function getPreferences() {
    var history = getHistory();
    var articles = history.articles;
    if (!articles.length) return null;

    // Count category frequency
    var catCounts = {};
    var entityCounts = {};
    var excludeIds = [];

    articles.forEach(function(a) {
      if (a.category) {
        catCounts[a.category] = (catCounts[a.category] || 0) + 1;
      }
      (a.entities || []).forEach(function(eid) {
        entityCounts[eid] = (entityCounts[eid] || 0) + 1;
      });
      excludeIds.push(a.id);
    });

    // Top 3 categories
    var topCats = Object.keys(catCounts).sort(function(a, b) {
      return catCounts[b] - catCounts[a];
    }).slice(0, 3);

    // Top 5 entities
    var topEntities = Object.keys(entityCounts).sort(function(a, b) {
      return entityCounts[b] - entityCounts[a];
    }).slice(0, 5).map(Number);

    return {
      categories: topCats,
      entities: topEntities,
      exclude: excludeIds.slice(-20)  // last 20 read articles
    };
  }

  /**
   * Load personalized recommendations via API.
   * @param {function} callback - called with array of article HTML or null
   */
  function loadRecommendations(callback) {
    var prefs = getPreferences();
    if (!prefs) {
      callback(null);
      return;
    }

    var params = [];
    if (prefs.categories.length) params.push('categories=' + prefs.categories.join(','));
    if (prefs.entities.length) params.push('entities=' + prefs.entities.join(','));
    if (prefs.exclude.length) params.push('exclude=' + prefs.exclude.join(','));

    var langPrefix = document.documentElement.lang === 'kz' ? '/kz' : '';
    var url = langPrefix + '/api/recommendations?' + params.join('&');

    fetch(url)
      .then(function(r) { return r.text(); })
      .then(function(html) {
        callback(html && html.trim() ? html : null);
      })
      .catch(function() {
        callback(null);
      });
  }

  // Expose API
  window.ReadingHistory = {
    track: trackArticle,
    getPreferences: getPreferences,
    loadRecommendations: loadRecommendations,
    getHistory: getHistory
  };
})();
