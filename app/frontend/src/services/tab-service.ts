import { Settings } from '@/components/settings/settings';
import { FlowTabContent } from '@/components/tabs/flow-tab-content';
import { DataCollectionPage } from '@/components/data-collection/data-collection-page';
import { WorkspacePage } from '@/components/workspace/workspace-page';
import { LiveTradingPage } from '@/components/live-trading/LiveTradingPage';
import { Flow } from '@/types/flow';
import { ReactNode, createElement } from 'react';

export interface TabData {
  type: 'flow' | 'settings' | 'data-collection' | 'workspace' | 'live-trading';
  title: string;
  flow?: Flow;
  metadata?: Record<string, any>;
}

export class TabService {
  static createTabContent(tabData: TabData): ReactNode {
    switch (tabData.type) {
      case 'flow':
        if (!tabData.flow) {
          throw new Error('Flow tab requires flow data');
        }
        return createElement(FlowTabContent, { flow: tabData.flow });

      case 'settings':
        return createElement(Settings);

      case 'data-collection':
        return createElement(DataCollectionPage);

      case 'workspace':
        return createElement(WorkspacePage);

      case 'live-trading':
        return createElement(LiveTradingPage);

      default:
        throw new Error(`Unsupported tab type: ${tabData.type}`);
    }
  }

  static createFlowTab(flow: Flow): TabData & { content: ReactNode } {
    return {
      type: 'flow',
      title: flow.name,
      flow: flow,
      content: TabService.createTabContent({ type: 'flow', title: flow.name, flow }),
    };
  }

  static createSettingsTab(): TabData & { content: ReactNode } {
    return {
      type: 'settings',
      title: 'Settings',
      content: TabService.createTabContent({ type: 'settings', title: 'Settings' }),
    };
  }

  static createDataCollectionTab(): TabData & { content: ReactNode } {
    return {
      type: 'data-collection',
      title: 'Data Collection',
      content: TabService.createTabContent({ type: 'data-collection', title: 'Data Collection' }),
    };
  }

  static createWorkspaceTab(): TabData & { content: ReactNode } {
    return {
      type: 'workspace',
      title: 'Workspace',
      content: TabService.createTabContent({ type: 'workspace', title: 'Workspace' }),
    };
  }

  static createLiveTradingTab(): TabData & { content: ReactNode } {
    return {
      type: 'live-trading',
      title: 'Live Trading',
      content: TabService.createTabContent({ type: 'live-trading', title: 'Live Trading' }),
    };
  }

  // Restore tab content for persisted tabs (used when loading from localStorage)
  static restoreTabContent(tabData: TabData): ReactNode {
    return TabService.createTabContent(tabData);
  }

  // Helper method to restore a complete tab from saved data
  static restoreTab(savedTab: TabData): TabData & { content: ReactNode } {
    switch (savedTab.type) {
      case 'flow':
        if (!savedTab.flow) {
          throw new Error('Flow tab requires flow data for restoration');
        }
        return TabService.createFlowTab(savedTab.flow);

      case 'settings':
        return TabService.createSettingsTab();

      case 'data-collection':
        return TabService.createDataCollectionTab();

      case 'workspace':
        return TabService.createWorkspaceTab();

      case 'live-trading':
        return TabService.createLiveTradingTab();

      default:
        throw new Error(`Cannot restore unsupported tab type: ${savedTab.type}`);
    }
  }
}

export class TabService {
  static createTabContent(tabData: TabData): ReactNode {
    switch (tabData.type) {
      case 'flow':
        if (!tabData.flow) {
          throw new Error('Flow tab requires flow data');
        }
        return createElement(FlowTabContent, { flow: tabData.flow });

      case 'settings':
        return createElement(Settings);

      case 'data-collection':
        return createElement(DataCollectionPage);

      case 'workspace':
        return createElement(WorkspacePage);

      default:
        throw new Error(`Unsupported tab type: ${tabData.type}`);
    }
  }

  static createFlowTab(flow: Flow): TabData & { content: ReactNode } {
    return {
      type: 'flow',
      title: flow.name,
      flow: flow,
      content: TabService.createTabContent({ type: 'flow', title: flow.name, flow }),
    };
  }

  static createSettingsTab(): TabData & { content: ReactNode } {
    return {
      type: 'settings',
      title: 'Settings',
      content: TabService.createTabContent({ type: 'settings', title: 'Settings' }),
    };
  }

  static createDataCollectionTab(): TabData & { content: ReactNode } {
    return {
      type: 'data-collection',
      title: 'Data Collection',
      content: TabService.createTabContent({ type: 'data-collection', title: 'Data Collection' }),
    };
  }

  static createWorkspaceTab(): TabData & { content: ReactNode } {
    return {
      type: 'workspace',
      title: 'Workspace',
      content: TabService.createTabContent({ type: 'workspace', title: 'Workspace' }),
    };
  }

  // Restore tab content for persisted tabs (used when loading from localStorage)
  static restoreTabContent(tabData: TabData): ReactNode {
    return TabService.createTabContent(tabData);
  }

  // Helper method to restore a complete tab from saved data
  static restoreTab(savedTab: TabData): TabData & { content: ReactNode } {
    switch (savedTab.type) {
      case 'flow':
        if (!savedTab.flow) {
          throw new Error('Flow tab requires flow data for restoration');
        }
        return TabService.createFlowTab(savedTab.flow);

      case 'settings':
        return TabService.createSettingsTab();

      case 'data-collection':
        return TabService.createDataCollectionTab();

      case 'workspace':
        return TabService.createWorkspaceTab();

      default:
        throw new Error(`Cannot restore unsupported tab type: ${savedTab.type}`);
    }
  }
}
