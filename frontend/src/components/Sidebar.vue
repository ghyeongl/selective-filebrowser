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

    <div v-if="isFiles && syncStats" class="storage-info">
      <progress-bar
        :max="syncStats.diskTotal"
        :segments="storageSegments"
        size="small"
        bg-color="#e9ecef"
        :bar-border-radius="3"
      />
      <div class="storage-summary">
        {{ diskUsedLabel }} of {{ diskTotalLabel }} used
      </div>
      <div class="seg-legend">
        <div><i class="dot archives"></i>Archives: {{ archivesLabel }}</div>
        <div><i class="dot spaces"></i>Spaces: {{ spacesLabel }}</div>
        <div><i class="dot other"></i>Other: {{ otherLabel }}</div>
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
    logout: auth.logout,
  },
  watch: {
    $route: {
      handler(to) {
        if (to.path.includes("/files")) {
          useSyncStore().fetchStats();
        }
      },
      immediate: true,
    },
  },
};
</script>

<style scoped>
.storage-info {
  padding: 1em 2.5em 0;
  margin: 1.5em 0 1em;
}

.storage-summary {
  font-size: 0.75em;
  color: #868e96;
  margin-top: 4px;
}

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

.dot.archives {
  background: #4dabf7;
}

.dot.spaces {
  background: #40c057;
}

.dot.other {
  background: #868e96;
}
</style>
