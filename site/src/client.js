const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
const dialog = $(".search-dialog");
const input = $("#search-input");
const results = $(".search-results");
const status = $(".search-status");
let index;
let loading;
let selected = -1;
let searchGeneration = 0;

function toast(message) {
  $(".toast").textContent = message;
  $(".toast").classList.add("visible");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => $(".toast").classList.remove("visible"), 2500);
}

async function getIndex() {
  if (index) return index;
  loading ||= fetch("/search-index.json")
    .then((response) => {
      if (!response.ok) throw new Error("Search index unavailable");
      return response.json();
    })
    .then((data) => {
      index = data;
      return data;
    })
    .catch((error) => {
      loading = undefined;
      throw error;
    });
  return loading;
}

function highlight(text, query) {
  const fragment = document.createDocumentFragment();
  const terms = query
    .split(/\s+/)
    .filter(Boolean)
    .map((term) => term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const expression = new RegExp(`(${terms.join("|")})`, "ig");
  let offset = 0;
  for (const match of text.matchAll(expression)) {
    fragment.append(text.slice(offset, match.index));
    const mark = document.createElement("mark");
    mark.textContent = match[0];
    fragment.append(mark);
    offset = match.index + match[0].length;
  }
  fragment.append(text.slice(offset));
  return fragment;
}

async function search() {
  const generation = ++searchGeneration;
  const query = input.value.trim().toLowerCase();
  results.replaceChildren();
  selected = -1;
  if (!query) {
    status.textContent =
      "Search endpoints, configuration variables, or a task.";
    return;
  }
  status.textContent = "Searching…";
  try {
    const entries = await getIndex();
    if (generation !== searchGeneration) return;
    const terms = query.split(/\s+/);
    const matches = entries
      .map((entry) => {
        const title = entry.title.toLowerCase();
        const haystack = `${title} ${entry.text.toLowerCase()}`;
        if (!terms.every((term) => haystack.includes(term))) return null;
        return {
          ...entry,
          score:
            terms.reduce(
              (score, term) => score + (title.includes(term) ? 20 : 1),
              0,
            ) + (entry.url.includes("#") ? 2 : 0),
        };
      })
      .filter(Boolean)
      .sort((a, b) => b.score - a.score)
      .slice(0, 30);
    status.textContent = matches.length
      ? `${matches.length === 30 ? "Top 30" : matches.length} results for “${input.value.trim()}”`
      : "No results. Try an endpoint name, setting, or shorter phrase.";
    for (const match of matches) {
      const link = document.createElement("a");
      link.href = match.url;
      link.className = "search-result";
      const group = document.createElement("span");
      group.className = "result-group";
      group.textContent = match.group;
      const title = document.createElement("strong");
      title.append(highlight(match.title, query));
      const excerpt = document.createElement("p");
      const foundAt = Math.max(
        0,
        match.text.toLowerCase().indexOf(terms[0]) - 50,
      );
      excerpt.append(
        highlight(
          `${foundAt ? "…" : ""}${match.text.slice(foundAt, foundAt + 190)}…`,
          query,
        ),
      );
      link.append(group, title, excerpt);
      link.addEventListener("click", () => dialog.close());
      results.append(link);
    }
  } catch {
    if (generation === searchGeneration)
      status.textContent =
        "Search could not load. Check your connection and try again; all guides remain available in navigation.";
  }
}

function openSearch() {
  closeMenu();
  if (!dialog.open) dialog.showModal();
  input.focus();
  search();
}
$(".search-trigger").addEventListener("click", openSearch);
$(".close-search").addEventListener("click", () => dialog.close());
dialog.addEventListener("click", (event) => {
  if (event.target === dialog) dialog.close();
});
input.addEventListener("input", search);
dialog.addEventListener("keydown", (event) => {
  const links = $$(".search-result");
  if (["ArrowDown", "ArrowUp"].includes(event.key) && links.length) {
    event.preventDefault();
    selected =
      (selected + (event.key === "ArrowDown" ? 1 : -1) + links.length) %
      links.length;
    links[selected].focus();
    links[selected].scrollIntoView({ block: "nearest" });
  }
  if (event.key === "Enter" && event.target === input && links.length) {
    event.preventDefault();
    links[Math.max(0, selected)].click();
  }
});
document.addEventListener("keydown", (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
    event.preventDefault();
    dialog.open ? dialog.close() : openSearch();
  }
});

$(".theme-toggle").addEventListener("click", () => {
  const theme =
    document.documentElement.dataset.theme === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = theme;
  try {
    localStorage.setItem("theme", theme);
  } catch {
    /* Private browsing can disable storage. */
  }
});

const menu = $(".menu-toggle");
const sidebar = $(".sidebar");
const shade = $(".nav-shade");
function closeMenu() {
  document.body.classList.remove("menu-open");
  menu.setAttribute("aria-expanded", "false");
  menu.setAttribute("aria-label", "Open navigation");
  shade.hidden = true;
  $(".workspace").inert = false;
}
menu.addEventListener("click", () => {
  if (menu.getAttribute("aria-expanded") === "true") {
    closeMenu();
    return;
  }
  document.body.classList.add("menu-open");
  menu.setAttribute("aria-expanded", "true");
  menu.setAttribute("aria-label", "Close navigation");
  shade.hidden = false;
  $(".workspace").inert = true;
  $('a[aria-current="page"]', sidebar)?.focus();
});
shade.addEventListener("click", closeMenu);
document.addEventListener("keydown", (event) => {
  if (menu.getAttribute("aria-expanded") !== "true") return;
  if (event.key === "Escape") {
    closeMenu();
    menu.focus();
  }
  if (event.key === "Tab") {
    const focusable = [menu, ...$$("a", sidebar)];
    const current = focusable.indexOf(document.activeElement);
    if (event.shiftKey && current <= 0) {
      event.preventDefault();
      focusable.at(-1).focus();
    } else if (!event.shiftKey && current === focusable.length - 1) {
      event.preventDefault();
      menu.focus();
    }
  }
});
matchMedia("(min-width: 901px)").addEventListener("change", (event) => {
  if (event.matches) closeMenu();
});
const activeNav = $('a[aria-current="page"]', sidebar);
if (activeNav && activeNav.offsetTop > sidebar.clientHeight - 100)
  sidebar.scrollTop = activeNav.offsetTop - sidebar.clientHeight / 2;

$$(".copy").forEach((button) =>
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(
        $("code", button.closest(".code-block")).textContent,
      );
      button.textContent = "Copied";
      toast("Code copied to clipboard");
      setTimeout(() => {
        button.textContent = "Copy";
      }, 2000);
    } catch {
      toast("Copy unavailable. Select and copy the code directly.");
    }
  }),
);

$$(".tabs").forEach((tablist) => {
  const tabs = $$('[role="tab"]', tablist);
  function activate(tab) {
    tabs.forEach((item) => {
      const active = item === tab;
      item.setAttribute("aria-selected", String(active));
      item.tabIndex = active ? 0 : -1;
      document.getElementById(item.getAttribute("aria-controls")).hidden =
        !active;
    });
  }
  tabs.forEach((tab, index) => {
    tab.addEventListener("click", () => activate(tab));
    tab.addEventListener("keydown", (event) => {
      let next;
      if (event.key === "ArrowRight") next = (index + 1) % tabs.length;
      if (event.key === "ArrowLeft")
        next = (index + tabs.length - 1) % tabs.length;
      if (event.key === "Home") next = 0;
      if (event.key === "End") next = tabs.length - 1;
      if (next !== undefined) {
        event.preventDefault();
        activate(tabs[next]);
        tabs[next].focus();
      }
    });
  });
});

if ("IntersectionObserver" in window) {
  const observer = new IntersectionObserver(
    (entries) => {
      const active = entries.find((entry) => entry.isIntersecting);
      if (!active) return;
      $$('.toc a[href^="#"]').forEach((link) =>
        link.classList.toggle("active", link.hash === `#${active.target.id}`),
      );
    },
    { rootMargin: "-80px 0px -65% 0px" },
  );
  $$("article h2[id]").forEach((heading) => observer.observe(heading));
}
