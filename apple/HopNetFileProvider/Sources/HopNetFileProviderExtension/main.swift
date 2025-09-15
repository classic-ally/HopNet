/*
HopNet FileProvider Extension - main.swift
Entry point for the file provider extension
*/

import Foundation
import FileProvider
import HopNetFileProviderCore

// This file is required for SPM to build the extension as an executable target.
// The actual entry point is NSExtensionMain, specified in the linker settings.
// The extension class is loaded via the NSExtensionPrincipalClass in Info.plist.

// Thin wrapper that inherits all functionality from the library base class
@objc(HopNetFileProviderExtension)
public class HopNetFileProviderExtension: HopNetFileProviderExtensionBase {
    // Inherits all functionality from base class - no additional code needed
}