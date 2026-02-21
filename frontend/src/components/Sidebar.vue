<template>
  <div v-show="active" @click="closeHovers" class="overlay"></div>
  <nav :class="{ active }">
    <template v-if="isLoggedIn">
      <button @click="toAccountSettings" class="action">
        <i class="material-icons">person</i>
        <span>{{ user.username }}</span>
      </button>
      <button
        class="action"
        @click="toRoot"
        :aria-label="$t('sidebar.myFiles')"
        :title="$t('sidebar.myFiles')"
      >
        <i class="material-icons">folder</i>
        <span>{{ $t("sidebar.myFiles") }}</span>
      </button>

      <div v-if="user.perm.create">
        <button
          @click="showHover('newDir')"
          class="action"
          :aria-label="$t('sidebar.newFolder')"
          :title="$t('sidebar.newFolder')"
        >
          <i class="material-icons">create_new_folder</i>
          <span>{{ $t("sidebar.newFolder") }}</span>
        </button>

        <button
          @click="showHover('newFile')"
          class="action"
          :aria-label="$t('sidebar.newFile')"
          :title="$t('sidebar.newFile')"
        >
          <i class="material-icons">note_add</i>
          <span>{{ $t("sidebar.newFile") }}</span>
        </button>
      </div>

      <div v-if="user.perm.admin">
        <button
          class="action"
          @click="toGlobalSettings"
          :aria-label="$t('sidebar.settings')"
          :title="$t('sidebar.settings')"
        >
          <i class="material-icons">settings_applications</i>
          <span>{{ $t("sidebar.settings") }}</span>
        </button>
      </div>
      <button
        v-if="canLogout"
        @click="logout"
        class="action"
        id="logout"
        :aria-label="$t('sidebar.logout')"
        :title="$t('sidebar.logout')"
      >
        <i class="material-icons">exit_to_app</i>
        <span>{{ $t("sidebar.logout") }}</span>
      </button>
    </template>
    <template v-else>
      <router-link
        v-if="!hideLoginButton"
        class="action"
        to="/login"
        :aria-label="$t('sidebar.login')"
        :title="$t('sidebar.login')"
      >
        <i class="material-icons">exit_to_app</i>
        <span>{{ $t("sidebar.login") }}</span>
      </router-link>

      <router-link
        v-if="signup"
        class="action"
        to="/login"
        :aria-label="$t('sidebar.signup')"
        :title="$t('sidebar.signup')"
      >
        <i class="material-icons">person_add</i>
        <span>{{ $t("sidebar.signup") }}</span>
      </router-link>
    </template>

    <div
      class="credits"
      v-if="isFiles && syncStats"
      style="width: 90%; margin: 2em 2.5em 3em 2.5em"
    >
      <progress-bar
        :max="syncStats.diskTotal"
        :segments="storageSegments"
        size="small"
        bg-color="#e9ecef"
        :bar-border-radius="3"
      />
      <br />
      {{ diskUsedLabel }} of {{ diskTotalLabel }} used
      <div class="seg-legend">
        <div><i class="dot" style="background:#4dabf7"></i>Archives: {{ archivesLabel }}</div>
        <div><i class="dot" style="background:#40c057"></i>Spaces: {{ spacesLabel }}</div>
        <div><i class="dot" style="background:#868e96"></i>Other: {{ otherLabel }}</div>
      </div>

      <div class="status-panel" v-if="syncStats.statusCounts">
        <div class="status-header">Queue: {{ syncStats.queueLen || 0 }}</div>
        <div class="status-row">
          <i class="dot" style="background:#868e96"></i>
          <span>archived</span>
          <span class="status-count">{{ formatCount(syncStats.statusCounts.archived) }}</span>
        </div>
        <div class="status-row">
          <i class="dot" style="background:#2b8a3e"></i>
          <span>synced</span>
          <span class="status-count">{{ formatCount(syncStats.statusCounts.synced) }}</span>
        </div>
        <div class="status-row">
          <i class="dot" style="background:#1864ab"></i>
          <span>syncing</span>
          <span class="status-count">{{ formatCount(syncStats.statusCounts.syncing) }}</span>
        </div>
        <div class="status-row">
          <i class="dot" style="background:#e67700"></i>
          <span>removing</span>
          <span class="status-count">{{ formatCount(syncStats.statusCounts.removing) }}</span>
        </div>
      </div>

      <div class="error-panel" v-if="syncStats.recentErrors && syncStats.recentErrors.length">
        <div v-for="(err, i) in syncStats.recentErrors" :key="i" class="error-row">
          {{ err.comp }}: {{ err.message }}<template v-if="err.error"> — {{ err.error }}</template>
        </div>
      </div>
    </div>

    <p class="credits">
      <span>
        <span v-if="disableExternal">File Browser</span>
        <a
          v-else
          rel="noopener noreferrer"
          target="_blank"
          href="https://github.com/filebrowser/filebrowser"
          >File Browser</a
        >
        <span> {{ " " }} {{ version }}</span>
      </span>
      <span>
        <a @click="help">{{ $t("sidebar.help") }}</a>
      </span>
    </p>
  </nav>
</template>

<script>
import { mapActions, mapState } from "pinia";
import { useAuthStore } from "@/stores/auth";
import { useFileStore } from "@/stores/file";
import { useLayoutStore } from "@/stores/layout";
import { useSyncStore } from "@/stores/sync";

import ProgressBar from "@/components/ProgressBar.vue";
import * as auth from "@/utils/auth";
import {
  version,
  signup,
  hideLoginButton,
  disableExternal,
  noAuth,
  logoutPage,
  loginPage,
} from "@/utils/constants";
import prettyBytes from "pretty-bytes";

export default {
  name: "sidebar",
  components: { ProgressBar },
  inject: ["$showError"],
  computed: {
    ...mapState(useAuthStore, ["user", "isLoggedIn"]),
    ...mapState(useFileStore, ["isFiles"]),
    ...mapState(useLayoutStore, ["currentPromptName"]),
    ...mapState(useSyncStore, { syncStats: "stats" }),
    active() {
      return this.currentPromptName === "sidebar";
    },
    signup: () => signup,
    hideLoginButton: () => hideLoginButton,
    version: () => version,
    disableExternal: () => disableExternal,
    canLogout: () => !noAuth && (loginPage || logoutPage !== "/login"),
    diskUsed() {
      if (!this.syncStats) return 0;
      return this.syncStats.diskTotal - this.syncStats.diskFree;
    },
    otherSize() {
      if (!this.syncStats) return 0;
      return Math.max(
        0,
        this.diskUsed -
          this.syncStats.archivesSize -
          this.syncStats.spacesSize
      );
    },
    storageSegments() {
      if (!this.syncStats) return [];
      return [
        { value: this.syncStats.archivesSize, color: "#4dabf7" },
        { value: this.syncStats.spacesSize, color: "#40c057" },
        { value: this.otherSize, color: "#868e96" },
      ];
    },
    diskUsedLabel() {
      return prettyBytes(this.diskUsed, { binary: true });
    },
    diskTotalLabel() {
      if (!this.syncStats) return "0 B";
      return prettyBytes(this.syncStats.diskTotal, { binary: true });
    },
    archivesLabel() {
      if (!this.syncStats) return "0 B";
      return prettyBytes(this.syncStats.archivesSize, { binary: true });
    },
    spacesLabel() {
      if (!this.syncStats) return "0 B";
      return prettyBytes(this.syncStats.spacesSize, { binary: true });
    },
    otherLabel() {
      return prettyBytes(this.otherSize, { binary: true });
    },
  },
  methods: {
    ...mapActions(useLayoutStore, ["closeHovers", "showHover"]),
    toRoot() {
      this.$router.push({ path: "/files" });
      this.closeHovers();
    },
    toAccountSettings() {
      this.$router.push({ path: "/settings/profile" });
      this.closeHovers();
    },
    toGlobalSettings() {
      this.$router.push({ path: "/settings/global" });
      this.closeHovers();
    },
    help() {
      this.showHover("help");
    },
    formatCount(n) {
      if (n == null) return "0";
      return n.toLocaleString();
    },
    logout: auth.logout,
  },
  watch: {
    $route: {
      handler(to) {
        const sync = useSyncStore();
        if (to.path.includes("/files")) {
          sync.startStatsPolling();
        } else {
          sync.stopStatsPolling();
        }
      },
      immediate: true,
    },
  },
  beforeUnmount() {
    useSyncStore().stopStatsPolling();
  },
};
</script>

<style scoped>
.seg-legend {
  font-size: 0.7em;
  color: #868e96;
  margin-top: 2px;
  line-height: 1.6;
}

.dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  margin-right: 4px;
  vertical-align: middle;
}

.status-panel {
  margin-top: 8px;
  font-size: 0.7em;
  color: #868e96;
  line-height: 1.6;
}

.status-header {
  font-weight: 600;
  color: #495057;
  margin-bottom: 2px;
}

.status-row {
  display: flex;
  align-items: center;
  gap: 4px;
}

.status-count {
  margin-left: auto;
  font-variant-numeric: tabular-nums;
}

.error-panel {
  margin-top: 8px;
  font-size: 0.65em;
  color: #c92a2a;
  line-height: 1.4;
  word-break: break-word;
}

.error-row {
  padding: 2px 0;
}
</style>
